-module(rabbit_stream_queue).

-include_lib("rabbit.hrl").
-include("amqqueue.hrl").

-export([
         init/1,
         apply/3,
         init_aux/1,
         handle_aux/6,

         %% client
         begin_stream/4,
         end_stream/2,
         credit/3,
         append/3,

         init_client/2,
         queue_name/1,
         handle_event/3,

         %% mgmt
         declare/1


         ]).

%% holds static or rarely changing fields
-record(cfg, {id :: ra:server_id(),
              name :: rabbit_types:r('queue')}).

-record(?MODULE, {cfg :: #cfg{},
                  last_index = 0 :: ra:index()}).

-opaque state() :: #?MODULE{}.
-type cmd() :: {append, Event :: term()}.
-export_type([state/0]).

-type stream_index() :: pos_integer().
-type stream_offset() :: non_neg_integer() | undefined.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


%% MACHINE

init(#{queue_name := QueueName,
       name := Name}) ->
    Cfg = #cfg{id = {Name, self()},
               name = QueueName},
    #?MODULE{cfg = Cfg}.

-spec apply(map(), cmd(), state()) ->
    {state(), stream_index(), list()}.
apply(#{index := RaftIndex}, {append, _Evt},
      #?MODULE{} = State) ->
    % rabbit_log:info("append ~b", [RaftIndex]),
    % Index = rabbit_stream_index:incr(RaftIndex, Index0),
    {State#?MODULE{last_index = RaftIndex}, RaftIndex, {aux, eval}}.

%% AUX

-type ctag() :: binary().

-type aux_cmd() :: {stream,
                    StartIndex :: stream_offset(),
                    MaxInFlight :: non_neg_integer(),
                    ctag(), pid()} |
                   {ack, {ctag(), pid()}, Index :: stream_index()} |
                   {stop_stream, pid()} |
                   eval.

-record(stream, {next_index :: ra:index(),
                 credit :: 0 | stream_index(),
                 max = 1000 :: non_neg_integer()}).

-type aux_state() :: #{{pid(), ctag()} => #stream{}}.

%% AUX

init_aux(_) ->
    #{}.

-spec handle_aux(term(), term(), aux_cmd(), aux_state(), Log, term()) ->
    {no_reply, aux_state(), Log} when Log :: term().
handle_aux(_RaMachine, _Type, {stream, Start, Max, Tag, Pid},
           Aux0, Log0, #?MODULE{cfg = Cfg,
                                last_index = Last} = _MacState) ->
    %% TODO: assert Pid is local and return error
    % rabbit_log:info("NEW STREAM: ~s", [Tag]),
    %% this works as a "skip to" function for exisiting streams. It does ignore
    %% any entries that are currently in flight
    %% read_cursor is the next item to read
    %% TODO: parse start offset and set accordingly
    LastOffset = case Start of
                     undefined ->
                         %% if undefined set offset to next offset
                         Last + 1;
                     _ -> Start
                 end,
    Str0 = #stream{next_index = max(1, LastOffset),
                   credit = Max,
                   max = Max},
    StreamId = {Tag, Pid},
    {Str, Log} = stream_entries(StreamId, Last, Cfg, Str0, Log0),
    AuxState = maps:put(StreamId, Str, Aux0),
    % rabbit_log:info("handle aux new stream for ~s", [Tag]),
    {no_reply, AuxState, Log, [{monitor, process, aux, Pid}]};
handle_aux(_RaMachine, _Type, {end_stream, Tag, Pid},
           Aux0, Log0, _MacState) ->
    StreamId = {Tag, Pid},
    {no_reply, maps:remove(StreamId, Aux0), Log0,
     [{monitor, process, aux, Pid}]};
handle_aux(_RaMachine, _Type, {credit, StreamId, Credit},
           Aux0, Log0, #?MODULE{cfg = Cfg,
                                last_index = Last} = _MacState) ->
   % rabbit_log:info("handle aux credit ~w", [Credit]),
    case Aux0 of
        #{StreamId := #stream{credit = Credit0} = Str0} ->
            %% update stream with ack value, constrain it not to be larger than
            %% the read index in case the streaming pid has skipped around in
            %% the stream by issuing multiple stream/3 commands.
            Str1 = Str0#stream{credit = Credit0 + Credit},
            {Str, Log} = stream_entries(StreamId, Last, Cfg, Str1, Log0),
            Aux = maps:put(StreamId, Str, Aux0),
            {no_reply, Aux, Log};
        _ ->
            {no_reply, Aux0, Log0}
    end;
handle_aux(_RaMachine, _Type, {down, Pid, _Info},
           Aux0, Log0, #?MODULE{cfg = _Cfg} = _MacState) ->
    %% remove all streams for the pid
    Aux = maps:filter(fun ({_Tag, P}, _) -> P =/= Pid end, Aux0),
    {no_reply, Aux, Log0};
handle_aux(_RaMachine, _Type, eval,
           Aux0, Log0,  #?MODULE{cfg = Cfg,
                                 last_index = Last} = _MacState) ->
    % rabbit_log:info("handle aux eval", []),
    {Aux, Log} = maps:fold(fun (StreamId, S0, {A0, L0}) ->
                                   {S, L} = stream_entries(StreamId, Last,
                                                           Cfg, S0, L0),
                                   {maps:put(StreamId, S, A0), L}
                           end, {#{}, Log0}, Aux0),
    {no_reply, Aux, Log}.

stream_entries({Tag, Pid} = StreamId,
               MaxIndex,
               #cfg{name = Name, id = Id} = Cfg,
               #stream{credit = Credit,
                       next_index = NextIdx} = Str0,
               Log0) when NextIdx =< MaxIndex ->
    %% e.g. min(101 + 50, 120) - 101
    MaxCredit = min(NextIdx + Credit, MaxIndex) - NextIdx,

    % rabbit_log:info("stream entries from ~b ~b ~b",
    %                 [NextIdx, MaxCredit, MaxIndex]),

    %% TODO: RA should provide a safe api for reading logs that
    case ra_log:take(NextIdx, MaxCredit, Log0) of
        {[], Log} ->
            {Str0, Log};
        {Entries0, Log} ->
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
            gen_server:cast(Pid, {stream_delivery, Tag, Msgs}),
            Str = Str0#stream{credit = Credit - NumMsgs,
                              next_index = NextIdx + NumEntries},
            % {Str, Log}
            case NumEntries == NumMsgs of
                true ->
                    %% we are done here
                    {Str, Log};
                false ->
                    %% if there are fewer Msgs than Entries0 it means there were non-events
                    %% in the log and we should recurse and try again
                    stream_entries(StreamId, MaxIndex, Cfg, Str, Log)
            end
    end;
stream_entries(_StreamId, _MaxIndex, _Cfg, Str, Log) ->
    {Str, Log}.

%% CLIENT

-type appender_seq() :: non_neg_integer().

-record(stream_client, {name :: term(),
                        leader = ra:server_id(),
                        local = ra:server_id(),
                        servers = [ra:server_id()],
                        next_seq = 1 :: non_neg_integer(),
                        correlation = #{} :: #{appender_seq() => term()}
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

handle_event(_From, {applied, SeqsReplies},
                      #stream_client{correlation = Correlation0} = State) ->
    {Seqs, _} = lists:unzip(SeqsReplies),
    Correlation = maps:without(Seqs, Correlation0),
    Corrs = maps:values(maps:with(Seqs, Correlation0)),
    {internal, Corrs, [],
     State#stream_client{correlation = Correlation}}.

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

begin_stream(#stream_client{local = ServerId} = State, Tag, Offset, MaxInFlight)
  when is_number(Offset) andalso is_number(MaxInFlight) ->
    Pid = self(),
    ok = ra:cast_aux_command(ServerId, {stream, Offset, MaxInFlight, Tag, Pid}),
    State.

end_stream(#stream_client{local = ServerId} = State, Tag) ->
    Pid = self(),
    ok = ra:cast_aux_command(ServerId, {end_stream, Tag, Pid}),
    State.


credit(#stream_client{local = ServerId} = State, Tag, Credit) ->
    ok = ra:cast_aux_command(ServerId, {credit, {Tag, self()}, Credit}),
    {ok, State}.

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
