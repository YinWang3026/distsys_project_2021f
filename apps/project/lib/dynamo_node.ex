defmodule DynamoNode do
  @moduledoc """
  A Dynamo node
  """

  # override Kernel's functions with Emulation's
  import Emulation, only: [send: 2, timer: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger

  alias ExHashRing.HashRing, as: Ring

  @type t() :: %__MODULE__{
    # Id for dynamo node
    id: any(),

    # Dynamo Parameters N, R and W
    n: pos_integer(),
    r: pos_integer(),
    w: pos_integer(),

    # Key Value store of each node
    store: %{required(any()) => {[any()], %Context{}}},

    # Record of other alive nodes in the cluster
    alive_nodes: %{required(any()) => boolean()},

    # Hash ring for the dynamo cluster
    ring: Ring.t(),

    # Timeout for failing client request
    client_timeout: pos_integer(),

    # Timeout for redirecting request to coordinator 
    redirect_timeout: pos_integer(),

    # Timeout for syncing using merkle tree
    merkle_sync_timeout: pos_integer(),

    # Number of seconds a node should wait for a response
    request_timeout: pos_integer(),

    # Number of seconds after which the node checks if other nodes are alive
    health_check_timeout: pos_integer(),

    # For coordinator, the number of gets to wait for before responsing to client (R)
    # {nonce => %{}}
    gets_queue: %{
        required(pos_integer()) => %{
            client: any(),
            key: any(),
            responses: %{required(any()) => {[any()], %Context{}}},
            requested: MapSet.t(any())
        }
    },

    # For coordinator, the number of puts to wait for before responsing to client (W)
    # {nonce => %{}}
    puts_queue: %{
        required(pos_integer()) => %{
                client: any(),
                key: any(),
                value: any(),
                context: %Context{},
                responses: MapSet.t(any()),
                requested: %{required(any()) => any() | nil},
                last_requested_index: non_neg_integer()
            }
    },

    # Pending redirect requests to be forwarded to the coordinator
    # {nonce => %{}}
    redirect_queue: %{
        required(pos_integer) => %{
            client: any(),
            msg: %ClientGetRequest{} | %ClientPutRequest{},
            get_or_put: :get | :put
          }
    },

    # Pending handoffs to be forwarded to the appropriate node
    # {node => %{}}
    handoffs_queue: %{
      required(any()) => %{
        required(pos_integer()) => %{
          required(any()) => %Context{}
        }
      }
    }
  }

  @enforce_keys [
    :id,
    :n,
    :r,
    :w,
    :store,
    :alive_nodes,
    :ring,
    :client_timeout,
    :redirect_timeout,
    :merkle_sync_timeout, 
    :request_timeout,
    :health_check_timeout,
    :gets_queue, 
    :puts_queue,
    :redirect_queue,
    :handoffs_queue
    ]
  # The state of each node
  defstruct([
    :id,
    :n,
    :r,
    :w,
    :store,
    :alive_nodes,
    :ring,
    :client_timeout,
    :redirect_timeout,
    :merkle_sync_timeout, 
    :request_timeout,
    :health_check_timeout,
    :gets_queue, 
    :puts_queue,
    :redirect_queue,
    :handoffs_queue
  ])

  @doc """
    Initialize dynamo node.
  """
  @spec init(
          any(),
          map(),
          [any()],
          pos_integer(),
          pos_integer(),
          pos_integer(),
          pos_integer(),
          pos_integer(),
          pos_integer(),
          pos_integer(),
          pos_integer()
        ) ::
          no_return()
  def init(
        id,
        data,
        nodes,
        n,
        r,
        w,
        client_timeout,
        redirect_timeout,
        request_timeout,
        health_check_timeout,
        merkle_sync_timeout
      ) do
    Logger.info("Starting node #{inspect(id)}")

    ring = Ring.new(nodes, 1)

    # store only the data that's assigned to this node
    store =
      data
      |> Enum.filter(fn {k, _v} ->
        id in Ring.find_nodes(ring, k, n)
      end)
      |> Map.new(fn {k, v} ->
        {k, {[v], %Context{version: VectorClock.new()}}}
      end)

    alive_nodes =
      nodes
      |> List.delete(id) # Take out self
      |> Map.new(fn node -> {node, true} end)

    state = %DynamoNode{
      id: id,
      n: n,
      r: r,
      w: w,
      store: store,
      alive_nodes: alive_nodes,
      ring: ring,
      client_timeout: client_timeout,
      redirect_timeout: redirect_timeout,
      request_timeout: request_timeout,
      health_check_timeout: health_check_timeout,
      merkle_sync_timeout: merkle_sync_timeout,
      gets_queue: %{},
      puts_queue: %{},
      redirect_queue: %{},
      handoffs_queue: %{}
    }

    # Start timers for health checkup and merkle sync
    timer(state.health_check_timeout, :health_check_timeout)
    timer(state.merkle_sync_timeout, :merkle_sync_timeout)

    # Start listening for requests
    listener(state)
  end

  @spec listener(%DynamoNode{}) :: no_return()
  def listener(state) do
    receive do
      # client get request
      {client, %ClientGetRequest{} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(client)}")

        # start timer for handling the client :get request
        timer(state.client_timeout, {:client_timeout, :get, msg.nonce})

        state =
          handle_client_request(
            state,
            msg,
            client,
            :get
          )

        listener(state)

      # client put request
      {client, %ClientPutRequest{} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(client)}")

        # start timer for handling the client :put request
        timer(state.client_timeout, {:client_timeout, :put, msg.nonce})

        state =
          handle_client_request(
            state,
            msg,
            client,
            :put
          )

        listener(state)

      # coordinator get request
      {coordinator, %CoordinatorGetRequest{nonce: nonce, key: key} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(coordinator)}")
        
        state = mark_alive(state, coordinator)
        stored = Map.get(state.store, key)

        {resp_values, resp_context} =
          case stored do
            {values, context} ->
              {values, context}

            nil ->
              {[], %Context{version: VectorClock.new()}}
          end

        send(coordinator, %CoordinatorGetResponse{
          nonce: nonce,
          values: resp_values,
          # remove the hint since the correct coordinator received it
          context: %{resp_context | hint: nil}
        })

        listener(state)

      {coordinator, %CoordinatorPutRequest{nonce: nonce, key: key,
        value: value, context: context} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(coordinator)}")

        state = mark_alive(state, coordinator)
        state = local_put(state, key, [value], context)

        send(coordinator, %CoordinatorPutResponse{
          nonce: nonce
        })

        if context.hint != nil and
             Map.get(state.alive_nodes, context.hint) == true do
          # try handing off hinted data
          state = handoff_hinted_data(state, context.hint)
          listener(state)
        else
          listener(state)
        end

      # node responses to coordinator requests
      {node, %CoordinatorGetResponse{} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")

        state = mark_alive(state, node)
        state = coordinator_get_response(state, node, msg)
        listener(state)

      {node, %CoordinatorPutResponse{} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")

        state = mark_alive(state, node)
        state = coordinator_put_response(state, node, msg)
        listener(state)

      # redirects from other nodes
      {node, %RedirectedClientRequest{
        client: client,
        request: %ClientGetRequest{nonce: nonce} = orig_msg
      } = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")

        state = mark_alive(state, node)
        # let redirecter know we're handling this request
        send(node, %RedirectAcknowledgement{nonce: nonce})
        # we must be the coordinator for this key
        state = coordinator_get_request(state, client, orig_msg)

        listener(state)

      {node, %RedirectedClientRequest{
        client: client,
        request: %ClientPutRequest{nonce: nonce} = orig_msg
      } = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")

        state = mark_alive(state, node)
        # let redirecter know we're handling this request
        send(node, %RedirectAcknowledgement{nonce: nonce})
        # we must be the coordinator for this key
        state = coordinator_put_request(state, client, orig_msg)

        listener(state)

      {node, %RedirectAcknowledgement{nonce: nonce} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")
        state = mark_alive(state, node)

        # redirect has been handled
        state = %{
          state
          | redirect_queue: Map.delete(state.redirect_queue, nonce)
        }

        listener(state)

      # timeouts
      # client request timeouts at coordinator
      {:client_timeout, :get, nonce} = msg ->
        Logger.info("Received #{inspect(msg)} from :client_timeout")

        # time up for redirect attempts, give up now
        state = 
          case Map.get(state.redirect_queue, nonce) do
            nil ->
              # request already been handled
              state
            true ->
              # redirect request still in queue, remove it
              %{
                state
                | redirect_queue: Map.delete(state.redirect_queue, nonce)
              }
          end

        req_state = Map.get(state.gets_queue, nonce)

        if req_state == nil do
          # request already handled
          listener(state)
        else
          # get rid of the pending entry and respond failure to client
          send(req_state.client, client_fail_msg(:get, nonce))

          listener(%{
            state
            | gets_queue: Map.delete(state.gets_queue, nonce)
          })
        end

      {:client_timeout, :put, nonce} = msg ->
        Logger.info("Received #{inspect(msg)} from :client_timeout")

        # time up for redirect attempts, give up now
        state = 
          case Map.get(state.redirect_queue, nonce) do
            nil ->
              # request already been handled
              state
            true ->
              # redirect request still in queue, remove it
              %{
                state
                | redirect_queue: Map.delete(state.redirect_queue, nonce)
              }
          end

        req_state = Map.get(state.puts_queue, nonce)

        if req_state == nil do
          # request already handled
          listener(state)
        else
          # get rid of the pending entry and respond failure to client
          send(req_state.client, client_fail_msg(:put, nonce))

          listener(%{
            state
            | puts_queue: Map.delete(state.puts_queue, nonce)
          })
        end

      # coord-request timeouts
      {:coordinator_request_timeout, :get, nonce, node} = msg ->
        Logger.info("Received #{inspect(msg)}")

        req_state = Map.get(state.gets_queue, nonce)

        # either the client request has been dealt with, or we've received
        # a coord-response from this node
        retry_not_required? =
          req_state == nil or Map.has_key?(req_state.responses, node)

        if retry_not_required? do
          listener(state)
        else
          # Since we didn't get a response in time, assume that node is dead.
          # Try to find another node
          state = mark_dead(state, node)

          %{key: key, requested: already_requested} = req_state

          # get first alive node we've not already requested
          all_nodes =
            Ring.find_nodes(
              state.ring,
              key,
              map_size(state.alive_nodes) + 1
            )

          new_node =
            Enum.find(all_nodes, fn node ->
              not MapSet.member?(already_requested, node) and
                (node == state.id or Map.get(state.alive_nodes, node) == true)
            end)

          if new_node != nil do
            # request this node
            send_with_timeout(
              state,
              new_node,
              %CoordinatorGetRequest{
                nonce: nonce,
                key: key
              },
              :request_timeout, # using request timeout
              {:coordinator_request_timeout, :get, nonce, new_node}
            )

            # update state accordingly
            new_req_state = %{
              req_state
              | requested: MapSet.put(already_requested, new_node)
            }

            state = %{
              state
              | gets_queue: Map.put(state.gets_queue, nonce, new_req_state)
            }

            listener(state)
          else
            # nobody else we can request, so don't retry
            listener(state)
          end
        end

      # coord-request timeouts for put
      {:coordinator_request_timeout, :put, nonce, node} = msg ->
        Logger.info("Received #{inspect(msg)}")

        req_state = Map.get(state.puts_queue, nonce)

        # either the client request has been dealt with, or we've received
        # a coord-response from this node
        retry_not_required? =
          req_state == nil or MapSet.member?(req_state.responses, node)

        if retry_not_required? do
          listener(state)
        else
          # Since we didn't get a response in time, assume that node is dead.
          # Try to find another node
          state = mark_dead(state, node)

          %{
            key: key,
            value: value,
            context: context,
            requested: already_requested,
            last_requested_index: last_requested_index
          } = req_state

          # get first alive node we've not already requested
          all_nodes =
            Ring.find_nodes(
              state.ring,
              key,
              map_size(state.alive_nodes) + 1
            )

          unrequested_nodes_ordered =
            Enum.drop(all_nodes, last_requested_index + 1)

          new_node =
            Enum.find(unrequested_nodes_ordered, fn node ->
              node == state.id or Map.get(state.alive_nodes, node) == true
            end)

          new_hint =
            case Map.get(already_requested, node) do
              nil ->
                # Since no hint, then timed out node *must* be in preference list
                # so the hint should be for this node
                node

              orig_hint ->
                # we transfer the hint to the new request
                orig_hint
            end

          if new_node != nil do
            # request this node
            send_with_timeout(
              state,
              new_node,
              %CoordinatorPutRequest{
                nonce: nonce,
                key: key,
                value: value,
                context: %{context | hint: new_hint}
              },
              :request_timeout, # using request timeout
              {:coordinator_request_timeout, :put, nonce, new_node}
            )

            # update state accordingly
            new_req_state = %{
              req_state
              | requested: Map.put(already_requested, new_node, new_hint),
                last_requested_index:
                  max(
                    req_state.last_requested_index,
                    Enum.find_index(all_nodes, &(&1 == new_node))
                  )
            }

            state = %{
              state
              | puts_queue: Map.put(state.puts_queue, nonce, new_req_state)
            }

            listener(state)
          else
            # nobody else we can request, so don't retry
            listener(state)
          end
        end

      {:redirect_timeout, nonce, failed_coord} = msg ->
        # redirect attempt failed, try again
        Logger.info("Received #{inspect(msg)}")

        if not Map.has_key?(state.redirect_queue, nonce) do
          # request already been handled successfully
          # or :client_timeout happened, and redirect request is removed
          listener(state)
        else
          # retry redirecting
          state = mark_dead(state, failed_coord)
          state = redirect_or_fail_client_request(state, nonce)
          listener(state)
        end

      # health checks
      :health_check_timeout = msg ->
        Logger.debug("Received #{inspect(msg)}")

        for {node, false} <- state.alive_nodes do
          send(node, :alive_check_request)
        end

        # restart the timer
        timer(state.health_check_timeout, :health_check_timeout)
        listener(state)

      {node, :alive_check_request = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")
        state = mark_alive(state, node)
        send(node, :alive_check_response)
        listener(state)

      {node, :alive_check_response = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")
        state = mark_alive(state, node)
        listener(state)

      # replica synchronization
      :merkle_sync_timeout = msg ->
        Logger.info("Received #{inspect(msg)}")
        #TODO: Add merkle tree logic

        # restart the timer
        timer(state.merkle_sync_timeout, :merkle_sync_timeout)
        listener(state)

      # testing
      {from, %GetStateRequest{nonce: nonce}} ->
        # respond with our current state
        send(from, %GetStateResponse{
          nonce: nonce,
          state: state
        })

        listener(state)

      # error
      unhandled_msg ->
        raise "Received unhandled msg: #{inspect(unhandled_msg)}"
    end
  end

  @doc """
  Send failure message to client
  """
  @spec client_fail_msg(:get | :put, pos_integer()) :: %ClientGetResponse{} | %ClientPutResponse{}
  def client_fail_msg(get_or_put, nonce) do
    if get_or_put == :get do
        %ClientGetResponse{
            nonce: nonce,
            success: false,
            values: nil,
            context: nil
          }
    else
        %ClientPutResponse{
            nonce: nonce,
            success: false,
            value: nil,
            context: nil
          }
    end
  end

  @doc """
  Utility function to remove outdated values from a list of {value, clock} pairs.
  """
  @spec merge_values({[any()], %Context{}}, {[any()], %Context{}}) ::
          {[any()], %Context{}}
  def merge_values({vals1, context1} = value1, {vals2, context2} = value2) do
    case Context.compare(context1, context2) do
      :before ->
        value2

      :after ->
        value1

      :concurrent ->
        all_vals =
          (vals1 ++ vals2)
          |> Enum.sort()
          |> Enum.dedup()

        {all_vals, Context.combine(context1, context2)}
    end
  end

  @doc """
  Add `key`-`value` association to local storage,
  squashing any outdated versions.
  """
  @spec local_put(%DynamoNode{}, any(), [any()], %Context{}) :: %DynamoNode{}
  def local_put(state, key, values, context) do
    Logger.debug("Writing #{inspect(values)} to key #{inspect(key)}")

    new_value = {values, context}

    new_store =
      Map.update(state.store, key, new_value, fn orig_value ->
        merge_values(new_value, orig_value)
      end)

    ## TODO ### 
    ## MERKLE TREE ####

    %{state | store: new_store}
  end

  @doc """
  Start retry timer and send message to process
  """
  @spec send_with_timeout(%DynamoNode{}, any(), any(), 
    :redirect_timeout | :request_timeout, any()) :: :ok
  def send_with_timeout(state, node, msg, timeout, timeout_msg) do
    if state.id == node do
        send(node, msg)
    else
        # create a timer with given timeout
        # timeout should be either request timeout, or redirect timeout
        timer(Map.get(state, timeout), timeout_msg)
        send(node, msg)
    end
  end

  @doc """
  Mark a node as dead.
  """
  @spec mark_dead(%DynamoNode{}, any()) :: %DynamoNode{}
  def mark_dead(state, node) do
    if state.id == node do
        state
    else
        %{state | alive_nodes: Map.replace!(state.alive_nodes, node, false)}
    end
  end


  @doc """
  Mark a node as alive
  """  
  @spec mark_alive(%DynamoNode{}, any()) :: %DynamoNode{}
  def mark_alive(state, node) do
    if state.id == node do
        state
    else
        state = %{
            state
            | alive_nodes: Map.replace!(state.alive_nodes, node, true)
          }
      
          # do pending operations for this node
          state = handoff_hinted_data(state, node)
      
          state
    end
  end

  @doc """
  Handoff data to node assuming it is alive.
  """
  @spec handoff_hinted_data(%DynamoNode{}, any()) :: %DynamoNode{}
  def handoff_hinted_data(state, node) do
    # data which we've tried to handoff and which we've not received
    # a response or a timeout for, yet
    # we should NOT try to hand this off again now
    node_handoffs_queue = Map.get(state.handoffs_queue, node, %{})

    handoff_queue_data =
      node_handoffs_queue
      |> Map.values()
      |> Enum.reduce(%{}, fn left, right ->
        Map.merge(left, right, fn _key, ctx1, ctx2 ->
          Context.combine(ctx1, ctx2)
        end)
      end)

    handoff_data =
      state.store
      |> Enum.filter(fn {_key, {_values, context}} ->
        context.hint == node
      end)
      # don't request pending handoff data again
      |> Enum.filter(fn {key, {_values, new_context}} ->
        pending_context = Map.get(handoff_queue_data, key)

        should_request? =
          pending_context == nil or
            Context.compare(new_context, pending_context) == :after

        should_request?
      end)
      # remove hint when handing off
      |> Enum.map(fn {key, {values, context}} ->
        {key, {values, %{context | hint: nil}}}
      end)
      |> Map.new()

    if Enum.empty?(handoff_data) do
      state
    else
      nonce = DynamoUtils.generate_nonce()

      send_with_timeout(
        state,
        node,
        %HandoffRequest{
          nonce: nonce,
          data: handoff_data
        },
        :request_timeout, # using request timeout
        {:handoff_timeout, nonce, node}
      )

      # make this request pending in state
      handoff_keys =
        Map.new(handoff_data, fn {key, {_value, context}} -> {key, context} end)

      new_node_handoffs_queue =
        Map.put(node_handoffs_queue, nonce, handoff_keys)

      %{
        state
        | handoffs_queue:
            Map.put(state.handoffs_queue, node, new_node_handoffs_queue)
      }
    end
  end

  @doc """
  Get the preference list (coordinators) for a particular key
  """
  @spec get_preference_list(%DynamoNode{}, any()) :: [any()]
  def get_preference_list(state, key) do
    Ring.find_nodes(state.ring, key, state.n)
  end

  @doc """
  Check if node is in the preference list
  """
  @spec is_valid_coordinator(%DynamoNode{}, any()) :: any()
  def is_valid_coordinator(state, key) do
    Enum.member?(get_preference_list(state, key), state.id)
  end

  @doc """
  Return the first valid coordinator, nil if none is found
  """
  @spec get_first_alive_coordinator(%DynamoNode{}, any()) :: any() | nil
  def get_first_alive_coordinator(state, key) do
    pref_list = get_preference_list(state, key)

    Enum.find(pref_list, nil, fn node ->
      node == state.id or state.alive_nodes[node] == true
    end)
  end

  @doc """
  Redirect, or reply failure to an incoming
  client request after a redirect failure.
  """
  @spec redirect_or_fail_client_request(%DynamoNode{}, pos_integer()) ::
          %DynamoNode{}
  def redirect_or_fail_client_request(state, nonce) do
    
    %{
      client: client,
      msg: received_msg,
      get_or_put: get_or_put
    } = Map.fetch!(state.redirect_queue, nonce)

    coord = get_first_alive_coordinator(state, received_msg.key)

    cond do
      coord != nil ->
        # redirect to coordinator
        send_with_timeout(
          state,
          coord,
          %RedirectedClientRequest{
            client: client,
            request: received_msg
          },
          :redirect_timeout, # using redirect timeout
          {:redirect_timeout, nonce, coord}
        )

        state

      coord == nil ->
        # no valid coordinator, reply failure
        send(client, client_fail_msg(get_or_put, nonce))

        # client request taken care of, no need to redirect anymore
        %{
          state
          | redirect_queue: Map.delete(state.redirect_queue, nonce)
        }
    end
  end

  @doc """
  Handle, redirect, or reply failure to an incoming client request.
  """
  @spec handle_client_request(%DynamoNode{}, %ClientGetRequest{} | %ClientPutRequest{}, 
    any(), :get | :put) :: %DynamoNode{}
  def handle_client_request(state, msg, client, get_or_put) do
    coord_handler =
      if get_or_put == :get do
        &coordinator_get_request/3
      else
        &coordinator_put_request/3
      end

    if is_valid_coordinator(state, msg.key) do
      # handle the request as coordinator
      coord_handler.(state, client, msg)
    else

      # put it in pending redirects
      state = %{
        state
        | redirect_queue:
            Map.put(state.redirect_queue, msg.nonce, %{
              client: client,
              msg: msg,
              get_or_put: get_or_put
            })
      }

      # redirect the request
      redirect_or_fail_client_request(state, msg.nonce)
    end
  end

  @doc """
  Return a list of the top 'n' healthy nodes for a particular key.
  """
  @spec get_alive_preference_list(%DynamoNode{}, any()) :: [any()]
  def get_alive_preference_list(state, key) do
    all_nodes =
      Ring.find_nodes(state.ring, key, map_size(state.alive_nodes) + 1)

    only_healthy =
      Enum.filter(all_nodes, fn node ->
        node == state.id or state.alive_nodes[node] == true
      end)

    Enum.take(only_healthy, state.n)
  end

  @doc """
  Respond to client's 'get' request as coordinator
  
  Request all versions of data from the top 'n' nodes in
  the preference list
  """
  @spec coordinator_get_request(%DynamoNode{}, any(), %ClientGetRequest{}) :: %DynamoNode{}
  def coordinator_get_request(state, client, %ClientGetRequest{
    nonce: nonce,
    key: key
  }) do

    alive_pref_list = get_alive_preference_list(state, key)

    Enum.each(alive_pref_list, fn node ->
      # DO send get request to self
      send_with_timeout(
        state,
        node,
        %CoordinatorGetRequest{
          nonce: nonce,
          key: key
        },
        :request_timeout, # using request timeout
        {:coordinator_request_timeout, :get, nonce, node}
      )
    end)

    %{ state
      | gets_queue:
      Map.put(state.gets_queue, nonce, %{
        client: client,
        key: key,
        responses: %{},
        requested: MapSet.new(alive_pref_list)
      })
    }
  end

  @doc """
  Handle get response as coordinator and send to client.

  Add it to the list of responses in 'gets_queue'.
  If we have 'R' or more responses for the corresponding client request,
  remove this request from 'gets_queue' and return all latest values to
  the client.
  """
  @spec coordinator_get_response(%DynamoNode{}, any(), %CoordinatorGetResponse{}) :: %DynamoNode{}
  def coordinator_get_response(state, node, %CoordinatorGetResponse{
    nonce: nonce,
    values: values,
    context: context
  }) do

    old_req_state = Map.get(state.gets_queue, nonce)

    new_req_state =
      if old_req_state == nil do
        nil
      else
        %{
          old_req_state
          | responses: Map.put(old_req_state.responses, node, {values, context})
        }
      end

    cond do
      new_req_state == nil ->
        # ignore this response
        # the request has been dealt with already
        state

      map_size(new_req_state.responses) >= state.r ->

        # enough responses, respond to client
        {latest_values, context} =
          new_req_state.responses
          |> Map.values()
          |> Enum.reduce(&merge_values/2)

        send(new_req_state.client, %ClientGetResponse{
          nonce: nonce,
          success: true,
          values: latest_values,
          context: context
        })

        # request not pending anymore, so get rid of the entry
        %{
          state
          | gets_queue: Map.delete(state.gets_queue, nonce)
        }

      true ->
        # not enough responses yet
        %{
          state
          | gets_queue: Map.put(state.gets_queue, nonce, new_req_state)
        }
    end
  end

  @doc """
  Respond to client's 'put' request as coordinator

  Send {key,value,vector_clock} to top `n` nodes in
  the preference list for key
  """
  @spec coordinator_put_request(%DynamoNode{}, any(), %ClientPutRequest{}) ::
          %DynamoNode{}
  def coordinator_put_request(state, client, %ClientPutRequest{
    nonce: nonce,
    key: key,
    value: value,
    context: context
  }) do

    context = %{context | version: VectorClock.tick(context.version, state.id)}

    # write to own store
    state = local_put(state, key, [value], context)
    # don't send put request to self
    to_request =
      get_alive_preference_list_with_intended(state, key)
      |> Enum.reject(fn {node, _hint} -> node == state.id end)

    to_request
    |> Enum.each(fn {node, hint} ->
      send_with_timeout(
        state,
        node,
        %CoordinatorPutRequest{
          nonce: nonce,
          key: key,
          value: value,
          context: %{context | hint: hint}
        },
        :request_timeout,
        {:coordinator_request_timeout, :put, nonce, node}
      )
    end)

    if state.w <= 1 do
      # we've already written once, so this is enough
      # respond to client, and don't mark this request as pending

      # we return the values so that the client can check if
      # their put request is persisted
      {resp_values, resp_context} = Map.get(state.store, key)

      send(client, %ClientPutResponse{
        nonce: nonce,
        success: true,
        value: resp_values,
        context: resp_context
      })

      state
    # else
    #   # otherwise, start timer for the responses and mark pending
    #   timer(
    #     state.coordinator_timeout,
    #     ## TODO### This timeout may be wrong
    #     {:total_coordinator_timeout, :put, nonce}
    #   )

    #   all_nodes =
    #     Ring.find_nodes(state.ring, key, map_size(state.alive_nodes) + 1)

    #   last_requested_index =
    #     to_request
    #     |> Enum.map(fn {node, _hint} ->
    #       Enum.find_index(all_nodes, &(&1 == node))
    #     end)
    #     |> Enum.max()

    #   %{
    #     state
    #     | puts_queue:
    #         Map.put(state.puts_queue, nonce, %{
    #           client: client,
    #           key: key,
    #           value: value,
    #           context: context,
    #           responses: MapSet.new(),
    #           requested: Map.new(to_request),
    #           last_requested_index: last_requested_index
    #         })
    #   }
    end
  end

  @doc """
  Handle put response as coordinator and send to client.

  Add it to the list of responses in 'puts_queue'.
  If we have 'W - 1' (-1 since we write to our local store as well)
  or more responses for the corresponding client request,
  remove this request from 'puts_queue' and return all latest values to
  the client.
  """
  @spec coordinator_put_response(%DynamoNode{}, any(), %CoordinatorPutResponse{}) ::
          %DynamoNode{}
  def coordinator_put_response(state, node, %CoordinatorPutResponse{
    nonce: nonce
  }) do

    old_req_state = Map.get(state.puts_queue, nonce)

    new_req_state =
      if old_req_state == nil do
        nil
      else
        %{
          old_req_state
          | responses: MapSet.put(old_req_state.responses, node)
        }
      end

    cond do
      new_req_state == nil ->
        # ignore this response
        # the request has been dealt with already
        state

      MapSet.size(new_req_state.responses) >= state.w - 1 ->

        # enough responses, respond to client
        {resp_values, resp_context} = Map.get(state.store, new_req_state.key)

        send(new_req_state.client, %ClientPutResponse{
          nonce: nonce,
          success: true,
          value: resp_values,
          context: resp_context
        })

        # request not pending anymore, so get rid of the entry
        %{
          state
          | puts_queue: Map.delete(state.puts_queue, nonce)
        }

      true ->
        # not enough responses yet
        %{
          state
          | puts_queue: Map.put(state.puts_queue, nonce, new_req_state)
        }
    end
  end

  @doc """
  Return a list of the top `n` healthy nodes for a particular key,
  along with the originally intended recipient (who's dead) and nil
  if it is the intended recipient.
  """
  @spec get_alive_preference_list_with_intended(%DynamoNode{}, any()) 
    :: [ {any(), any() | nil} ]
  def get_alive_preference_list_with_intended(state, key) do
    orig_pref_list = get_preference_list(state, key)
    alive_pref_list = get_alive_preference_list(state, key)

    dead_origs = orig_pref_list -- alive_pref_list

    unintendeds =
    Enum.filter(alive_pref_list, fn node -> node not in orig_pref_list end)

    hints = Map.new(Enum.zip(unintendeds, dead_origs))

    Enum.map(alive_pref_list, fn node ->
    {node, Map.get(hints, node)}
    end)
  end
end