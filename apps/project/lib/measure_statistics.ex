defmodule Params do
    
    alias __MODULE__

    @type t() :: %__MODULE__{
        duration: pos_integer(),
        request_rate:  {pos_integer(), pos_integer()},
        drop_rate: pos_integer(),
        mean_delay: pos_integer(),
        tt_fail: pos_integer(),
        tt_recover: pos_integer(),
        cluster_size: pos_integer(),
        num_keys: pos_integer(),
        num_clients: pos_integer(),
        n: pos_integer(),
        r: pos_integer(),
        w: pos_integer(),
        client_timeout: pos_integer(),
        redirect_timeout: pos_integer(),
        request_timeout: pos_integer(),
        health_check_timeout: pos_integer(),
        merkle_sync_timeout: pos_integer(),
    }
    
    @enforce_keys [:duration, :request_rate, :drop_rate, :mean_delay, :tt_fail]
    defstruct([
      :duration,
      :request_rate,
      :drop_rate,
      :mean_delay,
      :tt_fail,
      tt_recover: 1000,
      cluster_size: 100,
      num_keys: 100,
      num_clients: 5,
      n: 3,
      r: 2,
      w: 2,
      client_timeout: 300,
      redirect_timeout: 300,
      request_timeout: 700,
      health_check_timeout: 200,
      merkle_sync_timeout: 500
    ])
  end
  
  defmodule MeasureStatistics do
    import Emulation, only: [send: 2]
  
    import Kernel,
      except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
  
    require Logger
  
    def measure_output_csv(params) do
      # prevent all output
    #   Logger.remove_backend(:console)
  
      {:ok, dev} = StringIO.open("")
  
      original_dev = Process.group_leader()
      Process.group_leader(self(), dev)
      result = measure(params)
      Process.group_leader(self(), original_dev)
  
      IO.puts(
        "#{result.availability}, #{result.inconsistency}, #{result.stale_reads}"
      )
    end
  
    def measure(params) do
      Emulation.init()
      Emulation.mark_unfuzzable()
  
      Emulation.append_fuzzers([
        Fuzzers.delay_map(
            %{"CoordinatorGetRequest" => 5 / 1000,
            "CoordinatorGetResponse" => 10 / 1000,
            "CoordinatorPutRequest" => 10 / 1000,
            "CoordinatorPutResponse" => 10 / 1000}
            ),
        Fuzzers.drop(0)
      ])
  
      {:ok, crash_fuzzer} = CrashFuzzer.start()
  
      # make up values based on params
      data =
        for key <- 1..params.num_keys, into: %{} do
          {key, 10}
        end
  
      nodes =
        for node_num <- 1..params.cluster_size, into: [] do
          "node-#{node_num}"
        end
  
      contexts =
        for _client <- 1..params.num_clients, into: [] do
          %Context{version: VectorClock.new()}
        end
  
      {min_request_rate, max_request_rate} = params.request_rate
      min_request_interval = ceil(1000 / min_request_rate)
      max_request_interval = floor(1000 / max_request_rate)
  
      pids =
        DynamoUtils.new_cluster(
          data,
          nodes,
          params.n,
          params.r,
          params.w,
          params.client_timeout,
          params.redirect_timeout,
          params.request_timeout,
          params.health_check_timeout,
          params.merkle_sync_timeout
        )
  
      CrashFuzzer.enable(
        crash_fuzzer,
        params.tt_fail,
        params.tt_recover,
        pids
      )
  
      # initialize state
      state = %{
        last_written: data,
        num_inconsistencies: 0,
        num_stale_reads: 0,
        num_requests_failed: 0,
        num_requests_succeeded: 0,
        nodes: nodes,
        contexts: contexts,
        gets_queue: %{},
        puts_queue: %{},
        min_request_interval: min_request_interval,
        max_request_interval: max_request_interval
      }
  
      # start timer for how long we want to run this simulation
      # We cannot use Emulation.timer here here because this
      # process has not been spawned by Emulation,
      # so we use Process.send_after instead
    #   Process.send_after(self(), :measure_finish, params.duration)
  
    #   state = measure_loop(state)
      state = measure_loop_WARS(state, 50000)
  
      Emulation.terminate()
  
      # calculate stats
      total_requests =
        max(1, state.num_requests_failed + state.num_requests_succeeded)
  
      availability_percent =
        Float.round(state.num_requests_succeeded * 100 / total_requests, 2)
  
      inconsistency_percent =
        Float.round(state.num_inconsistencies * 100 / total_requests, 2)
  
      stale_reads_percent =
        Float.round(state.num_stale_reads * 100 / total_requests, 4)
      Logger.flush()
      
      Logger.warn("\n\n\n")
      Logger.warn("----------------------------")
      Logger.warn("    Measurements finished   ")
      Logger.warn("----------------------------")
      Logger.warn("Duration:        #{params.duration / 1000} s")
      Logger.warn("Request rate:    #{min_request_rate}-#{max_request_rate}/s")
      Logger.warn("Drop rate:       #{params.drop_rate * 100}%")
      Logger.warn("Mean delay:      #{params.mean_delay / 1000} s")
      Logger.warn("Mean TT fail:    #{params.tt_fail / 1000} s")
      Logger.warn("Mean TT recover: #{params.tt_recover / 1000} s")
      Logger.warn("----------------------------")
      Logger.warn("Total requests:  #{total_requests}")
      Logger.warn("Availability:    #{availability_percent}%")
      Logger.warn("Inconsistencies: #{inconsistency_percent}%")
      Logger.warn("P(Consistency): #{(total_requests - state.num_inconsistencies) / total_requests}")
      Logger.warn("Stale reads:     #{stale_reads_percent}%")
  
      %{
        availability: availability_percent,
        inconsistency: inconsistency_percent,
        stale_reads: stale_reads_percent
      }
    end
  
    def measure_loop(state) do
        
      state = send_random_client_request(state)
      state = handle_all_recvd_msgs(state)
      wait_before_next_request(state)
  
      if finished?() do
        state
      else
        measure_loop(state)
      end
    end


    def measure_loop_WARS(state, count) do
        state = send_client_write_read_request(state)
    
        if count == 0 do
          state
        else
            measure_loop_WARS(state, count-1)
        end
      end

  
    def create_random_client_request(state) do
  
      get_or_put = Enum.random([:get, :put])
      {key, last_value} = Enum.random(state.last_written)
      context_idx = Enum.random(1..Enum.count(state.contexts)) - 1
      context = Enum.at(state.contexts, context_idx)
  
      nonce = DynamoUtils.generate_nonce()
  
      if Map.has_key?(state.gets_queue, nonce) or
           Map.has_key?(state.puts_queue, nonce) do
        # This should almost never happen
        # In case it does, just try agian
        raise "Duplicate nonce!"
      end
  
      msg =
        case get_or_put do
          :get ->
            %ClientGetRequest{
              nonce: nonce,
              key: key
            }
  
          :put ->
            %ClientPutRequest{
              nonce: nonce,
              key: key,
              value: last_value + 10,
              context: context
            }
        end
  
      {msg, context_idx}
    end

    def create_random_client_request_with_request_type(state, req_type) do
  
        {key, last_value} = Enum.random(state.last_written)
        context_idx = Enum.random(1..Enum.count(state.contexts)) - 1
        context = Enum.at(state.contexts, context_idx)
    
        nonce = DynamoUtils.generate_nonce()
    
        if Map.has_key?(state.gets_queue, nonce) or
             Map.has_key?(state.puts_queue, nonce) do
          # This should almost never happen
          # In case it does, just try agian
          raise "Duplicate nonce!"
        end
    
        msg =
          case req_type do
            :get ->
              %ClientGetRequest{
                nonce: nonce,
                key: key
              }
    
            :put ->
              %ClientPutRequest{
                nonce: nonce,
                key: key,
                value: last_value + 10,
                context: context
              }
          end
    
        {msg, context_idx, key, nonce}
      end

    def send_client_write_read_request(state) do
        node = Enum.random(state.nodes)

        # Send a write request
        {msg, context_idx, _key, nonce} = create_random_client_request_with_request_type(state, :put)
        send(node, msg)

        state = %{
            state
            | puts_queue:
                Map.put(state.puts_queue, nonce, %{
                  msg: msg,
                  context_idx: context_idx
                })
          }

        state = handle_all_recvd_msgs(state)
        
        # Wait for DELTA seconds
        :timer.sleep(1)

        # Send a read request
        {msg, context_idx, key, nonce} = create_random_client_request_with_request_type(state, :get)
        send(node, msg)

        state = %{
            state
            | gets_queue:
                Map.put(state.gets_queue, nonce, %{
                  expected_value: Map.fetch!(state.last_written, key),
                  msg: msg,
                  context_idx: context_idx
                })
          }
        
        state = handle_all_recvd_msgs(state)
        state
    end

    def send_random_client_request(state) do
      node = Enum.random(state.nodes)
      {msg, context_idx} = create_random_client_request(state)
  
      Logger.warn("Sending: #{inspect(msg, pretty: true)} to #{inspect(node)}")
      send(node, msg)
  
      case msg do
        %ClientGetRequest{nonce: nonce, key: key} ->
          %{
            state
            | gets_queue:
                Map.put(state.gets_queue, nonce, %{
                  expected_value: Map.fetch!(state.last_written, key),
                  msg: msg,
                  context_idx: context_idx
                })
          }
  
        %ClientPutRequest{nonce: nonce} ->
          %{
            state
            | puts_queue:
                Map.put(state.puts_queue, nonce, %{
                  msg: msg,
                  context_idx: context_idx
                })
          }
      end
    end
  
    def handle_recvd_msg(state, msg) do
      pending_map =
        case msg do
          %ClientGetResponse{} -> state.gets_queue
          %ClientPutResponse{} -> state.puts_queue
        end
  
      if not Map.has_key?(pending_map, msg.nonce) do
        # we're receiving a duplicate response, ignore
        state
      else
        case msg do
          %ClientGetResponse{
            nonce: nonce,
            success: success,
            values: values,
            context: context
          } ->
            {%{
               expected_value: expected_value,
               msg: _msg,
               context_idx: context_idx
             }, new_gets_queue} = Map.pop!(state.gets_queue, nonce)
  
            state = %{state | gets_queue: new_gets_queue}
  
            if success == false do
              %{
                state
                | num_requests_failed: state.num_requests_failed + 1
              }
            else
              # update context at context_idx
              updated_contexts =
                List.update_at(state.contexts, context_idx, fn _ctx ->
                  context
                end)
  
              inconsistency? = Enum.count(values) > 1
              # NOTE: We assume the following to NOT be a stale read:
              #   write 10
              #   write 20
              #   write 30
              #   read
              #   write 40
              #   * get read response values = [10, 40] while expecting 30
              stale_read? =
                Enum.all?(
                  values,
                  fn recvd_value -> recvd_value < expected_value end
                )
  
              %{
                state
                | num_requests_succeeded: state.num_requests_succeeded + 1,
                  num_inconsistencies:
                    state.num_inconsistencies + if(inconsistency?, do: 1, else: 0),
                  num_stale_reads:
                    state.num_stale_reads + if(stale_read?, do: 1, else: 0),
                  contexts: updated_contexts
              }
            end
  
          %ClientPutResponse{
            nonce: nonce,
            success: success,
            value: resp_values,
            context: context
          } ->
            {%{
               msg: msg,
               context_idx: context_idx
             }, new_puts_queue} = Map.pop!(state.puts_queue, nonce)
  
            state = %{state | puts_queue: new_puts_queue}
  
            if success == false do
              %{
                state
                | num_requests_failed: state.num_requests_failed + 1
              }
            else
              # update context at context_idx
              updated_contexts =
                List.update_at(state.contexts, context_idx, fn _ctx ->
                  context
                end)
  
              # potentially update last_written
              update_last_written =
                if msg.value in resp_values do
                  # if the version we sent is concurrent with the version we got back
                  # (or even *after*,but that should not be possible)
                  # then we know the value has been persisted
                  Map.update!(
                    state.last_written,
                    msg.key,
                    &max(msg.value, &1)
                  )
                else
                  state.last_written
                end
  
              %{
                state
                | num_requests_succeeded: state.num_requests_succeeded + 1,
                  contexts: updated_contexts,
                  last_written: update_last_written
              }
            end
        end
      end
    end
  
    def handle_all_recvd_msgs(state) do
      # go over all recvd messages
      all_recvd_msgs = recv_all_msgs_in_mailbox()
      Logger.warn("#{Enum.count(all_recvd_msgs)} messages received")
  
      for msg <- all_recvd_msgs, reduce: state do
        state_acc ->
          Logger.warn("Received: #{inspect(msg, pretty: true)}")
          handle_recvd_msg(state_acc, msg)
      end
    end
  
    def wait_before_next_request(state) do
      # Wait for next request timeout
      next_request_timeout =
        Enum.random(state.min_request_interval..state.max_request_interval)
  
      receive do
      after
        next_request_timeout -> true
      end
    end
  
    def finished?() do
      receive do
        :measure_finish -> true
      after
        0 -> false
      end
    end
  
    @doc """
    Receive all responses except for simulation finish msg.
    """
    def recv_all_msgs_in_mailbox() do
      receive do
        msg when msg != :measure_finish ->
          [msg | recv_all_msgs_in_mailbox()]
      after
        0 -> []
      end
    end
  end
  