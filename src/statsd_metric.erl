-module(statsd_metric).

-export([new/4, encode/1]).

-define(IS_TYPE(X), X =:= counter; X =:= timer; X =:= gauge; X =:= set).

-record(metric, {name :: name(),
                 value :: value(),
                 type :: type(),
                 opts :: options()}).

-type name() :: binary().
-type value() :: integer().
-type type() :: counter | timer | gauge | set.
-type options() :: #{sample := float(), tags := map()}.
-opaque metric() :: #metric{}.
-type statsd_line_data() :: binary().

-export_type([metric/0]).

%%%=============================================================================
%%% API
%%%=============================================================================
-spec new(name(), value(), type(), options()) -> metric().
new(Name, Value, Type, Opts)
  when is_binary(Name), is_integer(Value), ?IS_TYPE(Type), is_map(Opts) ->
    #metric{name = Name, value = Value, type = Type, opts = Opts}.

-spec encode(metric()) -> statsd_line_data().
encode(#metric{name = Name, value = Value, type = Type, opts = Opts}) ->
    Data = [serialize_metric(Name, Value, Type),
            serialize_sample_rate(Opts) |
            serialize_tags(Opts)],
    erlang:iolist_to_binary(Data).

%%%=============================================================================
%%% Helper functions
%%%=============================================================================
-spec serialize_metric(name(), value(), type()) -> binary().
serialize_metric(MetricName, Value, Type) ->
    <<(format_name(MetricName))/bytes, $:,
      (format_value(Value))/bytes, $|,
      (format_type(Type))/bytes>>.

-spec serialize_sample_rate(options()) -> binary().
serialize_sample_rate(#{sample := Rate}) when is_float(Rate) ->
    <<"|@", (erlang:float_to_binary(Rate, [{decimals, 1}]))/bytes>>;
serialize_sample_rate(_) ->
    <<>>.

-spec serialize_tags(options()) -> list().
serialize_tags(#{tags := Tags}) when is_map(Tags), map_size(Tags) > 0 ->
    Serializer = fun(K, V, Acc) -> [serialize_tag(K, V) | Acc] end,
    [<<"|#">> | lists:join($,, maps:fold(Serializer, [], Tags))];
serialize_tags(#{}) ->
    [].

-spec format_name(name()) -> binary().
format_name(Name) ->
    sanitize(Name, [<<":">>, <<"|">>, <<"@">>]).

-spec format_value(value()) -> binary().
format_value(Value) ->
    erlang:integer_to_binary(Value).

-spec format_type(type()) -> binary().
format_type(counter) -> <<"c">>;
format_type(timer) -> <<"ms">>;
format_type(gauge) -> <<"g">>;
format_type(set) -> <<"s">>.

serialize_tag(K, V) ->
    Key = sanitize(ensure_binary(K), [<<":">>, <<",">>]),
    Val = sanitize(ensure_binary(V), [<<":">>, <<",">>]),
    <<Key/bytes, $:, Val/bytes>>.

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

sanitize(BinText, SpecialChars) ->
    binary:replace(BinText, SpecialChars, <<"_">>, [global]).
