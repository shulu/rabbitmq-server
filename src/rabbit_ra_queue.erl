-module(rabbit_ra_queue).

-export([
         recover/1,
         force_delete/1
         ]).

%% holds static or rarely changing fields




recover(Queues) ->
    [begin
         {Name, _} = amqqueue:get_pid(Q0),
         case ra:restart_server({Name, node()}) of
             ok ->
                 % queue was restarted, good
                 ok;
             {error, Err1}
               when Err1 == not_started orelse
                    Err1 == name_not_registered ->
                 % queue was never started on this node
                 % so needs to be started from scratch.
                 TickTimeout = application:get_env(rabbit, quorum_tick_interval,
                                                   5000),
                 Type = amqqueue:get_type(Q0),
                 Conf = Type:make_ra_conf(Q0, {Name, node()}, TickTimeout),
                 case ra:start_server(Conf) of
                     ok ->
                         ok;
                     Err2 ->
                         rabbit_log:warning("recover: quorum queue ~w could not"
                                            " be started ~w", [Name, Err2]),
                         ok
                 end;
             {error, {already_started, _}} ->
                 %% this is fine and can happen if a vhost crashes and performs
                 %% recovery whilst the ra application and servers are still
                 %% running
                 ok;
             Err ->
                 %% catch all clause to avoid causing the vhost not to start
                 rabbit_log:warning("recover: quorum queue ~w could not be "
                                    "restarted ~w", [Name, Err]),
                 ok
         end,
         %% we have to ensure the  quorum queue is
         %% present in the rabbit_queue table and not just in rabbit_durable_queue
         %% So many code paths are dependent on this.
         {ok, Q} = rabbit_amqqueue:ensure_rabbit_queue_record_is_initialized(Q0),
         Q
     end || Q0 <- Queues].


force_delete(Servers) ->
    [begin
         case catch(ra:force_delete_server(S)) of
             ok -> ok;
             Err ->
                 rabbit_log:warning(
                   "Force delete of ~w failed with: ~w"
                   "This may require manual data clean up~n",
                   [S, Err]),
                 ok
         end
     end || S <- Servers],
    ok.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
