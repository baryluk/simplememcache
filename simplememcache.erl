-module(simplememcache).
-author('baryluk@smp.if.uj.edu.pl').

% Copyright 2010, Witold Baryluk
% Licencja BSD


-export([start/0, start/1, server/1, process/5, loop/2]).
-export([add/2, get/1, add/3, get/2]).


-define(PORT, 41728).
-define(IP, {127,0,0,1}).


start() ->
	start(?PORT).

start(Port) ->
	spawn(?MODULE, server, [Port]).

server(Port) ->
	Tab = ets:new(?MODULE, [public, set, {write_concurrency, true}]),
	{ok, Socket} = gen_udp:open(Port, [binary, {recbuf, 4096}]), % default is 8192
	loop(Tab, Socket).

loop(Tab, Socket) ->
	receive
		{udp, Socket, Src_IP, Src_Port, Packet} ->
			spawn(?MODULE, process, [Tab, Socket, Src_IP, Src_Port, Packet]),
			loop(Tab, Socket);
		Other ->
			io:format("Other message ~p~n", [Other]),
			?MODULE:loop(Tab, Socket)
	end.

process(Tab, Socket, Src_IP, Src_Port, Packet) ->
	Data = case binary_to_term(Packet, [safe]) of
		{add, Key, Value} ->
			ets:insert(Tab, {Key, Value});
		{get, Key} ->
			ets:lookup(Tab, Key)
	end,
	BackPacket = term_to_binary(Data, [{compressed,3},{minor_version,1}]),
	ok = gen_udp:send(Socket, Src_IP, Src_Port, BackPacket).


add(K, V) ->
	add(?IP, K, V).

add(IP, K, V) ->
	client(IP, {add, K, term_to_binary("dupablada",[{compressed,8},{minor_version,1}])}).

get(K) ->
	get(?IP, K).

get(IP, K) ->
	case client(IP, {get, K}) of
		[{_,V}] ->
			{value, binary_to_term(V, [safe])};
		[] ->
			none
	end.

client(Op) ->
	client({127,0,0,1},Op).

client(IP, Op) ->
	client(IP, ?PORT, Op).

client(IP, Port, Data) ->
	{ok, Socket} = gen_udp:open(0, [binary, {recbuf, 4096}]), % default is 8192
	Packet = term_to_binary(Data, [{compressed,6}]),
	ok = gen_udp:send(Socket, IP, Port, Packet),
	receive
		{udp, Socket, IP, Port, BackPacket} ->
			binary_to_term(BackPacket, [safe])
		after 10000 ->
			timeout
	end.
