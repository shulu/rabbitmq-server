-module(rabbit_stream_queue).

-include_lib("rabbit.hrl").
-include("amqqueue.hrl").

-export([
         init/1,
         apply/3,
         tick/2,
         init_aux/1,
         handle_aux/6,

         %% client
         begin_stream/4,
         end_stream/2,
         credit/3,
         append/3,

         init_client/2,
         queue_name/1,
         pending_size/1,
         handle_event/3,

         %% mgmt
         declare/1,
         delete/2,

         %% other
         open_files/1,
         make_ra_conf/4,
         cluster_state/1


         ]).

-define(DELETE_TIMEOUT, 5000).
-define(STATISTICS_KEYS,
        [
         policy,
         % operator_policy,
         % effective_policy_definition,
         consumers,
         memory,
         state,
         garbage_collection,
         leader,
         online,
         members,
         open_files
         % single_active_consumer_pid,
         % single_active_consumer_ctag,
         % messages_ram,
         % message_bytes_ram
        ]).
%% segment sizing is hard
-define(SEG_MAX_MSGS, 100000).
%% avoid creating ridiculously small segments
-define(SEG_MIN_MSGS, 4096).
-define(SEG_MAX_BYTES, 1000 * 1000 * 500). %% 500Mb
-define(SEG_MAX_MS, 1000 * 60 * 60). %% 1hr

-record(retention_spec,
        {max_bytes = ?SEG_MAX_BYTES * 4 :: non_neg_integer(),
         max_ms :: undefined | non_neg_integer()}).

%% holds static or rarely changing fields
-record(cfg, {id :: ra:server_id(),
              name :: rabbit_types:r('queue'),
              retention :: #retention_spec{}}).

%% a log segment
-record(seg, {from_system_time_ms :: non_neg_integer(),
              to_system_time_ms :: non_neg_integer(),
              from_idx :: ra:index(),
              to_idx :: ra:index(),
              num_msgs = 0 :: non_neg_integer(),
              num_bytes = 0 :: non_neg_integer()}).

-record(?MODULE, {cfg :: #cfg{},
                  log_segments = [] :: [#seg{}],
                  last_index = 0 :: ra:index()}).

-opaque state() :: #?MODULE{}.
-type cmd() :: {append, Event :: term()}.
-export_type([state/0]).

-type stream_index() :: pos_integer().
-type stream_offset() :: non_neg_integer() | undefined.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% calculate_log_stats(Segs) ->
%     ok.


%% MACHINE

init(#{queue_name := QueueName,
       name := Name}) ->
    Cfg = #cfg{id = {Name, self()},
               retention = #retention_spec{},
               name = QueueName},
    #?MODULE{cfg = Cfg}.

-spec apply(map(), cmd(), state()) ->
    {state(), stream_index(), list()}.
apply(#{index := RaftIndex} = Meta, {append, Msg},
      #?MODULE{log_segments = Segs0} = State) ->
    % rabbit_log:info("append ~b", [RaftIndex]),
    Segs = incr_log_segment(Meta, Segs0, Msg),
    {State#?MODULE{last_index = RaftIndex,
                   log_segments = Segs}, RaftIndex}.

-spec tick(non_neg_integer(), state()) -> ra_machine:effects().
tick(_Ts, #?MODULE{cfg = #cfg{id = {Name, _},
                              name = QName},
                   log_segments = Segs} = _State) ->
    % CheckoutBytes = 0,
    {NumMsgs, NumBytes}  = lists:foldl(fun(#seg{num_msgs = N, num_bytes = B},
                                           {N0, B0}) ->
                                               {N0 + N, B0 +B}
                                       end, {0, 0}, Segs),
    Infos = [
             {message_bytes_ready, NumBytes},
             {message_bytes_unacknowledged, 0},
             {message_bytes, NumBytes},
             {message_bytes_persistent, NumBytes},
             {messages_persistent, NumMsgs}
             | infos(QName)],
    rabbit_core_metrics:queue_stats(QName, Infos),
    R = reductions(Name),
    rabbit_core_metrics:queue_stats(QName, NumMsgs, 0, NumMsgs, R),
    [].

%% AUX

-type ctag() :: binary().

-type aux_cmd() :: {stream,
                    StartIndex :: stream_offset(),
                    MaxInFlight :: non_neg_integer(),
                    ctag(), pid()} |
                   {ack, {ctag(), pid()}, Index :: stream_index()} |
                   {stop_stream, pid()} |
                   %% built in
                   eval |
                   tick.

%% stream,undefined,53,0,97,-1,1,
-record(stream, {name :: rabbit_types:r('queue'),
                 next_index :: ra:index(),
                 min_index = 0 :: ra:index(),
                 max_index = 0 :: ra:index(),
                 credit :: non_neg_integer(),
                 max = 1000 :: non_neg_integer(),
                 log :: undefined | ra_log_reader:state()}).

-type aux_state() :: #{{pid(), ctag()} => #stream{}}.

%% AUX

init_aux(_) ->
    #{}.

-spec handle_aux(term(), term(), aux_cmd(), aux_state(), Log, term()) ->
    {no_reply, aux_state(), Log} when Log :: term().
handle_aux(_RaMachine, {call, _}, {register_reader, _Tag, Pid},
           Aux0, Log0, #?MODULE{last_index = LastIdx} = _MacState) ->
    %% TODO Tag needs to included in register

    {LastWritten, _} = ra_log:last_written(Log0),
    Last = min(LastWritten, LastIdx),

    {Log, Effs} = ra_log:register_reader(Pid, Log0),
    {reply, Last, Aux0, Log, [{monitor, process, aux, Pid} | Effs]};
handle_aux(_RaMachine, _Type, {end_stream, Tag, Pid},
           Aux0, Log0, _MacState) ->
    StreamId = {Tag, Pid},
    {no_reply, maps:remove(StreamId, Aux0), Log0,
     [{monitor, process, aux, Pid}]};
handle_aux(_RaMachine, _Type, {down, Pid, _Info},
           Aux0, Log0, #?MODULE{cfg = _Cfg} = _MacState) ->
    %% remove all streams for the pid
    Aux = maps:filter(fun ({_Tag, P}, _) -> P =/= Pid end, Aux0),
    {no_reply, Aux, Log0};
handle_aux(_RaMachine, _Type, eval,
           Aux0, Log0,  #?MODULE{cfg = _Cfg,
                                 last_index = Last} = _MacState) ->
    %% Emit max index for all log readers
    {LastWritten, _} = ra_log:last_written(Log0),
    MaxIdx = min(LastWritten, Last),
    Effs = [{send_msg, P, {max_index, MaxIdx}, [ra_event, local]}
            || P <- ra_log:readers(Log0)],
    {no_reply, Aux0, Log0, Effs};
handle_aux(_RaMachine, _Type, tick,
           Aux0, Log0, _MacState) ->
    {no_reply, Aux0, Log0}.

stream_entries(Name, Id, Str) ->
    stream_entries(Name, Id, Str, []).

stream_entries(Name, Id,
               #stream{
                       credit = Credit,
                       max_index = MaxIdx,
                       next_index = NextIdx,
                       log = Log0} = Str0, MsgIn)
  when NextIdx =< MaxIdx ->
    To = min(NextIdx + Credit - 1, MaxIdx),

    % rabbit_log:info("stream entries from ~b to ~b ~w ~w",
    %                 [NextIdx, To, Log0, ets:tab2list(ra_log_open_mem_tables)]),
    case ra_log_reader:read(NextIdx, To, Log0) of
        {[], _, Log} ->
            % rabbit_log:info("stream entries none out", []),
            {Str0#stream{log = Log}, []};
        {Entries0, _, Log} ->
            % rabbit_log:info("stream entries got entries", [Entries0]),
            %% filter non usr append commands out
            Msgs = [begin
                        Msg = rabbit_basic:add_header(<<"x-stream-offset">>,
                                                      long, Idx, Msg0),
                        {Name, Id, Idx, false, Msg}
                    end
                    || {Idx, _, {'$usr', _, {append, Msg0}, _}} <- Entries0],
            NumEntries = length(Entries0),
            NumMsgs = length(Msgs),

            %% as all deliveries should be local we don't need to use
            %% nosuspend and noconnect here
            % gen_server:cast(Pid, {stream_delivery, Tag, Msgs}),
            Str = Str0#stream{credit = Credit - NumMsgs,
                              log = Log,
                              next_index = NextIdx + NumEntries},
            % {Str, Log}
            case NumEntries == NumMsgs of
                true ->
                    %% we are done here
                    % rabbit_log:info("stream entries out ~w ~w", [Entries0, Msgs]),
                    {Str, MsgIn ++ Msgs};
                false ->
                    %% if there are fewer Msgs than Entries0 it means there were non-events
                    %% in the log and we should recurse and try again
                    stream_entries(Name, Id, Str, MsgIn ++ Msgs)
            end
    end;
stream_entries(_Name, _Id, Str, Msgs) ->
    % rabbit_log:info("stream entries none ~b ~b", [Str#stream.next_index,
    %                                         Str#stream.max_index
    %                                        ]),
    {Str, Msgs}.

%% CLIENT

-type appender_seq() :: non_neg_integer().

-record(stream_client, {name :: term(),
                        leader = ra:server_id(),
                        local = ra:server_id(),
                        servers = [ra:server_id()],
                        next_seq = 1 :: non_neg_integer(),
                        correlation = #{} :: #{appender_seq() => term()},
                        readers = #{} :: #{term() => #stream{}}
                       }).

init_client(QueueName, ServerIds) when is_list(ServerIds) ->
    {ok, _, Leader} = ra:members(hd(ServerIds)),
    [Local | _] = [L || {_, Node} = L <- ServerIds, Node == node()],
    #stream_client{name = QueueName,
                   leader = Leader,
                   local = Local,
                   servers = ServerIds}.

queue_name(#stream_client{name = Name}) ->
    Name.

pending_size(#stream_client{correlation = Correlation}) ->
    maps:size(Correlation).

handle_event(_From, {applied, SeqsReplies},
                      #stream_client{correlation = Correlation0} = State) ->
    {Seqs, _} = lists:unzip(SeqsReplies),
    Correlation = maps:without(Seqs, Correlation0),
    Corrs = maps:values(maps:with(Seqs, Correlation0)),
    {internal, Corrs, [],
     State#stream_client{correlation = Correlation}};
handle_event(_From, {machine, {ra_log_update, UId, MinIdx, SegRefs}},
                      #stream_client{name = Name,
                                     local = Local,
                                     readers = Readers0} = State) ->
    % rabbit_log:info("handle event ~w ~w", [MinIdx, SegRefs]),
    Readers = maps:map(
                fun(Tag, #stream{log = Log} = S0) ->
                        L = case Log of
                                undefined ->
                                    ra_log_reader:init(UId, 1, SegRefs);
                                L0 ->
                                    ra_log_reader:handle_log_update(MinIdx,
                                                                    SegRefs, L0)
                            end,
                        Str0 = S0#stream{log = L, min_index = MinIdx},
                        {Str, Msgs} = stream_entries(Name, Local, Str0),
                        %% HACK for now, better to just return but tricky with acks credits
                        %% that also evaluate the stream
                        gen_server:cast(self(), {stream_delivery, Tag, Msgs}),
                        Str
                end, Readers0),
    % {{delivery, Tag, Msgs},
    %  State#stream_client{readers = Readers0#{Tag => Str}}}.
    {internal, [], [],
     State#stream_client{readers = Readers}};
handle_event(_From, {machine, {max_index, MaxIdx}},
                      #stream_client{name = Name,
                                     local = Local,
                                     readers = Readers0} = State) ->
    Readers = maps:map(
                fun(Tag, S0) ->
                        Str0 = S0#stream{max_index = MaxIdx},
                        {Str, Msgs} = stream_entries(Name, Local, Str0),
                        %% HACK for now, better to just return but tricky with acks credits
                        %% that also evaluate the stream
                        gen_server:cast(self(), {stream_delivery, Tag, Msgs}),
                        Str
                end, Readers0),
    {internal, [], [],
     State#stream_client{readers = Readers}}.

append(#stream_client{leader = ServerId,
                      next_seq = Seq,
                      correlation = Correlation0} = State, MsgId, Event) ->
    ok = ra:pipeline_command(ServerId, {append, Event}, Seq),
    Correlation = case MsgId of
                      undefined ->
                          Correlation0;
                      _ when is_number(MsgId) ->
                          Correlation0#{Seq => MsgId}
                  end,
    State#stream_client{next_seq = Seq + 1,
                        correlation = Correlation}.

begin_stream(#stream_client{local = ServerId,
                            readers = Readers0} = State,
             Tag, Offset, Max)
  when is_number(Max) ->
    Pid = self(),
    Last = ra:aux_command(ServerId, {register_reader, Tag, Pid}),
    LastOffset = case Offset of
                     undefined ->
                         %% if undefined set offset to next offset
                         Last + 1;
                     _ -> Offset
                 end,
    Str0 = #stream{next_index = max(1, LastOffset),
                   credit = Max,
                   max_index = Last,
                   max = Max},
    % StreamId = {Tag, Pid},
    State#stream_client{readers = Readers0#{Tag => Str0}}.

end_stream(#stream_client{local = ServerId} = State, Tag) ->
    Pid = self(),
    ok = ra:cast_aux_command(ServerId, {end_stream, Tag, Pid}),
    State.


credit(#stream_client{name = Name,
                      local = Local,
                      readers = Readers0} = State, Tag, Credit) ->
    Readers = case Readers0 of
                  #{Tag := #stream{credit = Credit0} = Str0} ->
                      Str1 = Str0#stream{credit = Credit0 + Credit},
                      {Str, Msgs} = stream_entries(Name, Local, Str1),
                      %% HACK for now, better to just return but
                      %% tricky with acks credits
                      %% that also evaluate the stream
                      gen_server:cast(self(), {stream_delivery, Tag, Msgs}),
                      Readers0#{Tag => Str};
                  _ ->
                      Readers0
              end,
    {ok, State#stream_client{readers = Readers}}.

%% MGMT

declare(Q0) ->
    QName = amqqueue:get_name(Q0),
    Name = qname_to_rname(QName),
    Arguments = amqqueue:get_arguments(Q0),
    Opts = amqqueue:get_options(Q0),
    ActingUser = maps:get(user, Opts, ?UNKNOWN_USER),
    LocalId = {Name, node()},
    Nodes = rabbit_mnesia:cluster_nodes(all),
    Q1 = amqqueue:set_type_state(amqqueue:set_pid(Q0, LocalId),
                                 #{nodes => Nodes}),
    ServerIds =  [{Name, Node} || Node <- Nodes],
    case rabbit_amqqueue:internal_declare(Q1, false) of
        {created, NewQ} ->
            TickTimeout = application:get_env(rabbit,
                                              quorum_tick_interval,
                                              5000),
            RaConfs = [make_ra_conf(NewQ, ServerId, ServerIds, TickTimeout)
                       || ServerId <- ServerIds],
            case ra:start_cluster(RaConfs) of
                {ok, _, _} ->
                    rabbit_event:notify(queue_created,
                                        [{name, QName},
                                         {durable, true},
                                         {auto_delete, false},
                                         {arguments, Arguments},
                                         {user_who_performed_action,
                                          ActingUser}]),
                    {new, NewQ};
                {error, Error} ->
                    _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
                    rabbit_misc:protocol_error(
                      internal_error,
                      "Cannot declare a queue '~s' on node '~s': ~255p",
                      [rabbit_misc:rs(QName), node(), Error])
            end;
        {existing, _} = Ex ->
            Ex
    end.


delete(Q, ActingUser) when ?amqqueue_is_stream(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    QName = amqqueue:get_name(Q),
    QNodes = get_nodes(Q),
    Timeout = ?DELETE_TIMEOUT,
    {ok, ReadyMsgs, _} = stat(Q),
    Servers = [{Name, Node} || Node <- QNodes],
    case ra:delete_cluster(Servers, Timeout) of
        {ok, {_, LeaderNode} = Leader} ->
            MRef = erlang:monitor(process, Leader),
            receive
                {'DOWN', MRef, process, _, _} ->
                    ok
            after Timeout ->
                    ok = rabbit_ra_queue:force_delete(Servers)
            end,
            ok = delete_queue_data(QName, ActingUser),
            rpc:call(LeaderNode, rabbit_core_metrics, queue_deleted, [QName],
                     1000),
            {ok, ReadyMsgs};
        {error, {no_more_servers_to_try, Errs}} ->
            case lists:all(fun({{error, noproc}, _}) -> true;
                              (_) -> false
                           end, Errs) of
                true ->
                    %% If all ra nodes were already down, the delete
                    %% has succeed
                    delete_queue_data(QName, ActingUser),
                    {ok, ReadyMsgs};
                false ->
                    %% attempt forced deletion of all servers
                    rabbit_log:warning(
                      "Could not delete quorum queue '~s', not enough nodes "
                       " online to reach a quorum: ~255p."
                       " Attempting force delete.",
                      [rabbit_misc:rs(QName), Errs]),
                    ok = rabbit_ra_queue:force_delete(Servers),
                    delete_queue_data(QName, ActingUser),
                    {ok, ReadyMsgs}
            end
    end.
qname_to_rname(#resource{virtual_host = <<"/">>, name = Name}) ->
    erlang:binary_to_atom(<<"%2F_", Name/binary>>, utf8);
qname_to_rname(#resource{virtual_host = VHost, name = Name}) ->
    erlang:binary_to_atom(<<VHost/binary, "_", Name/binary>>, utf8).

make_ra_conf(Q, ServerId, ServerIds, TickTimeout) ->
    QName = amqqueue:get_name(Q),
    RaMachine = ra_machine(Q),
    [{ClusterName, _} | _]  = ServerIds,
    UId = ra:new_uid(ra_lib:to_binary(ClusterName)),
    FName = rabbit_misc:rs(QName),
    #{cluster_name => ClusterName,
      id => ServerId,
      uid => UId,
      friendly_name => FName,
      metrics_key => QName,
      initial_members => ServerIds,
      log_init_args => #{uid => UId},
      tick_timeout => TickTimeout,
      machine => RaMachine}.

ra_machine(Q) ->
    QName = amqqueue:get_name(Q),
    {module, ?MODULE, #{queue_name => QName}}.


message_size(#basic_message{content = Content}) ->
    #content{payload_fragments_rev = PFR} = Content,
    iolist_size(PFR);
message_size(B) when is_binary(B) ->
    byte_size(B);
message_size(Msg) ->
    %% probably only hit this for testing so ok to use erts_debug
    erts_debug:size(Msg).

incr_log_segment(#{index := Idx,
                   system_time := Time}, [], Msg) ->
    Bytes = message_size(Msg),
    [#seg{from_system_time_ms = Time,
          to_system_time_ms = Time,
          from_idx = Idx,
          to_idx = Idx,
          num_msgs = 1,
          num_bytes = Bytes}];
incr_log_segment(#{index := Idx,
                   system_time := Time},
                 [#seg{from_system_time_ms = SegTime,
                       num_msgs = NumMsgs0,
                       num_bytes = NumBytes0} = Seg | Segs] = AllSegs, Msg) ->
    Bytes = message_size(Msg),
    NumBytes = NumBytes0 + Bytes,
    NumMsgs = NumMsgs0 + 1,
    %% check if a new segment should be created
    case NumMsgs > ?SEG_MIN_MSGS andalso
         (NumMsgs > ?SEG_MAX_MSGS orelse
          NumBytes > ?SEG_MAX_BYTES orelse
          Time - SegTime > ?SEG_MAX_MS) of
        true ->
            %% time for a new segment
            [#seg{from_system_time_ms = Time,
                  to_system_time_ms = Time,
                  from_idx = Idx,
                  to_idx = Idx,
                  num_msgs = 1,
                  num_bytes = Bytes} | AllSegs];
        false ->
            [Seg#seg{to_idx = Idx,
                     to_system_time_ms = Time,
                     num_msgs = NumMsgs,
                     num_bytes = NumBytes} | Segs]
    end.

reductions(Name) ->
    try
        {reductions, R} = process_info(whereis(Name), reductions),
        R
    catch
        error:badarg ->
            0
    end.

infos(QName) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            info(Q, ?STATISTICS_KEYS);
        {error, not_found} ->
            []
    end.

-spec info(amqqueue:amqqueue(), rabbit_types:info_keys()) -> rabbit_types:infos().

info(Q, Items) ->
    [{Item, i(Item, Q)} || Item <- Items].

i(name,        Q) when ?is_amqqueue(Q) -> amqqueue:get_name(Q);
i(durable,     Q) when ?is_amqqueue(Q) -> amqqueue:is_durable(Q);
i(auto_delete, Q) when ?is_amqqueue(Q) -> amqqueue:is_auto_delete(Q);
i(arguments,   Q) when ?is_amqqueue(Q) -> amqqueue:get_arguments(Q);
i(pid, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    whereis(Name);
i(messages, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, _, _, M, _}] ->
            M;
        [] ->
            0
    end;
i(messages_ready, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, MR, _, _, _}] ->
            MR;
        [] ->
            0
    end;
i(messages_unacknowledged, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, _, MU, _, _}] ->
            MU;
        [] ->
            0
    end;
i(policy, Q) ->
    case rabbit_policy:name(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(operator_policy, Q) ->
    case rabbit_policy:name_op(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(effective_policy_definition, Q) ->
    case rabbit_policy:effective_definition(Q) of
        undefined -> [];
        Def       -> Def
    end;
i(consumers, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_metrics, QName) of
        [{_, M, _}] ->
            proplists:get_value(consumers, M, 0);
        [] ->
            0
    end;
i(memory, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    try
        {memory, M} = process_info(whereis(Name), memory),
        M
    catch
        error:badarg ->
            0
    end;
i(state, Q) when ?is_amqqueue(Q) ->
    {Name, Node} = amqqueue:get_pid(Q),
    %% Check against the leader or last known leader
    case rpc:call(Node, ?MODULE, cluster_state, [Name], 1000) of
        {badrpc, _} -> down;
        State -> State
    end;
i(local_state, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    case ets:lookup(ra_state, Name) of
        [{_, State}] -> State;
        _ -> not_member
    end;
i(garbage_collection, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    try
        rabbit_misc:get_gc_info(whereis(Name))
    catch
        error:badarg ->
            []
    end;
i(members, Q) when ?is_amqqueue(Q) ->
    get_nodes(Q);
i(online, Q) ->
    get_nodes(Q);
i(leader, Q) ->
    {_Name, Leader} = amqqueue:get_pid(Q),
    Leader;
i(open_files, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    Nodes = get_nodes(Q),
    {Data, _} = rpc:multicall(Nodes, ?MODULE, open_files, [Name]),
    lists:flatten(Data);
i(single_active_consumer_pid, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    {ok, {_, SacResult}, _} = ra:local_query(QPid,
                                             fun rabbit_fifo:query_single_active_consumer/1),
    case SacResult of
        {value, {_ConsumerTag, ChPid}} ->
            ChPid;
        _ ->
            ''
    end;
i(single_active_consumer_ctag, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    {ok, {_, SacResult}, _} = ra:local_query(QPid,
                                             fun rabbit_fifo:query_single_active_consumer/1),
    case SacResult of
        {value, {ConsumerTag, _ChPid}} ->
            ConsumerTag;
        _ ->
            ''
    end;
i(type, _) -> quorum;
i(messages_ram, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    {ok, {_, {Length, _}}, _} = ra:local_query(QPid,
                                          fun rabbit_fifo:query_in_memory_usage/1),
    Length;
i(message_bytes_ram, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    {ok, {_, {_, Bytes}}, _} = ra:local_query(QPid,
                                         fun rabbit_fifo:query_in_memory_usage/1),
    Bytes;
i(_K, _Q) -> ''.

get_nodes(Q) when ?is_amqqueue(Q) ->
    #{nodes := Nodes} = amqqueue:get_type_state(Q),
    Nodes.

open_files(Name) ->
    case whereis(Name) of
        undefined -> {node(), 0};
        Pid -> case ets:lookup(ra_open_file_metrics, Pid) of
                   [] -> {node(), 0};
                   [{_, Count}] -> {node(), Count}
               end
    end.

cluster_state(Name) ->
    case whereis(Name) of
        undefined -> down;
        _ ->
            case ets:lookup(ra_state, Name) of
                [{_, recover}] -> recovering;
                _ -> running
            end
    end.
delete_queue_data(QName, ActingUser) ->
    _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
    ok.

stat(_Q) ->
    {ok, 0, 0}.
