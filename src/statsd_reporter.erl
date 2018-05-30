-module(statsd_reporter).

-behaviour(exometer_report).

-export([exometer_init/1,
         exometer_report/5,
         exometer_subscribe/5,
         exometer_unsubscribe/4,
         exometer_info/2,
         exometer_call/3,
         exometer_cast/2,
         exometer_terminate/2,
         exometer_setopts/4,
         exometer_newentry/2]).

-record(state, {socket :: inet:socket(),
                address :: inet:socket_address() | inet:hostname(),
                port :: inet:port_number(),
                tags :: #{}}).

%%====================================================================
%% API functions
%%====================================================================
exometer_init(Opts) ->
    Host = exometer_util:get_opt(host, Opts),
    Port = exometer_util:get_opt(port, Opts),
    Tags = exometer_util:get_opt(tags, Opts, #{}),
    case gen_udp:open(0) of
        {ok, Socket} ->
            {ok, #state{socket = Socket, address = Host, port = Port,
                        tags = Tags}};
        Error ->
            Error
    end.

exometer_report(Metric, DataPoint, undefined, Value, State) ->
    exometer_report(Metric, DataPoint, #{}, Value, State);
exometer_report(Metric, DataPoint, Extra, Value, #state{tags = GlobalTags} = S) ->
    Name = format_name(Metric, DataPoint, Extra),
    Tags = maps:merge(GlobalTags, maps:get(tags, Extra, #{})),
    Packet = [Name, $:, encode_value(Value), "|g" | encode_tags(Tags)],
    gen_udp:send(S#state.socket, S#state.address, S#state.port, Packet),
    {ok, S}.

exometer_subscribe(_Metric, _DataPoint, _Extra, _Interval, State) ->
    {ok, State}.

exometer_unsubscribe(_Metric, _DataPoint, _Extra, State) ->
    {ok, State}.

exometer_info(_, State) ->
    {ok, State}.

exometer_call(_, _, State) ->
    {ok, State}.

exometer_cast(_, State) ->
    {ok, State}.

exometer_terminate(_, _) ->
    ignore.

exometer_setopts(_Metric, _Options, _Status, State) ->
    {ok, State}.

exometer_newentry(_entry, State) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================
format_name(Metric, DataPoint, #{series_name := {template, Temp}}) ->
    Formatter = fun(X, []) ->
                        [get_name_token(X, Metric, DataPoint)];
                   (X, Acc) ->
                        [get_name_token(X, Metric, DataPoint), $. | Acc]
                end,
    lists:reverse(lists:foldl(Formatter, [], Temp));
format_name(_Metric, _DataPoint, #{series_name := Name}) ->
    encode_name(Name);
format_name(Metric, DataPoint, _) ->
    lists:map(fun(X) -> [encode_name(X), $.] end, Metric) ++
        [encode_name(DataPoint)].

get_name_token(X, Metric, _DataPoint) when is_integer(X) ->
    encode_name(lists:nth(X, Metric));
get_name_token(dp, _Metric, DP) ->
    encode_name(DP).

encode_name(Data) ->
    binary:replace(ensure_binary(Data), [<<":">>, <<"|">>, <<"@">>], <<"_">>, [global]).

encode_value(X) when is_integer(X) ->
    erlang:integer_to_binary(X);
encode_value(X) when is_float(X) ->
    erlang:float_to_binary(X).

encode_tags(Tags) ->
    Encoder =
        fun(K, V, []) ->
                [<<(encode_tag(K))/bytes, $:, (encode_tag(V))/bytes>>, $#];
           (K, V, Acc) ->
                [<<(encode_tag(K))/bytes, $:, (encode_tag(V))/bytes>>, $, | Acc]
        end,
    lists:reverse(maps:fold(Encoder, [], Tags)).

encode_tag(X) ->
    binary:replace(ensure_binary(X), [<<":">>, <<",">>], <<"_">>, [global]).

ensure_binary(X) when is_atom(X) ->
    erlang:atom_to_binary(X, latin1);
ensure_binary(X) when is_list(X) ->
    erlang:list_to_binary(X);
ensure_binary(X) when is_binary(X) ->
    X;
ensure_binary(X) when is_integer(X) ->
    erlang:integer_to_binary(X);
ensure_binary(X) when is_float(X) ->
    erlang:float_to_binary(X).
