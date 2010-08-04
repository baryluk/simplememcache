-module(simplememcache).
-author('baryluk@smp.if.uj.edu.pl').

% Copyright 2010, Witold Baryluk
% Licencja BSD

% for older ERTS:  remove write_concurrency, and [safe]


%-define(B2T(X), binary_to_term(X, [safe])).
-define(B2T(X), binary_to_term(X)).
-define(T2B(X), term_to_binary(X, [{minor_version,1}])).
-define(T2B_CLIENT(X), term_to_binary(X, [compressed, {minor_version,1}])).

-export([start/0, start/1, server/1, process/5, loop/3]).
-export([add/2, get/1, add/3, get/2, async_add/3, async_add/2]).


-define(PORT, 41728).
-define(IP, {10,0,2,5}).
%-define(OtherOpts, [{write_concurrency, true}]).
-define(OtherOpts, []).


start() ->
	start(?PORT).

start(Port) ->
	P = spawn(?MODULE, server, [Port]),
%	timer:send_interval(1000, P, stat),
	P.

server(Port) ->
	Tab = ets:new(?MODULE, [public, set, named_table] ++ ?OtherOpts),
	{ok, Socket} = gen_udp:open(Port, [binary, {recbuf, 4096}, {reuseaddr, true}]), % default is 8192
	loop(0, Tab, Socket).

loop(I, Tab, Socket) ->
	receive
		{udp, Socket, Src_IP, Src_Port, Packet} ->
			spawn(?MODULE, process, [Tab, Socket, Src_IP, Src_Port, Packet]),
			loop(I+1, Tab, Socket);
		Other ->
			Info = ets:info(Tab),
			io:format("Other message ~p. I=~p Info=~p~n", [Other, I, Info]),
			?MODULE:loop(I, Tab, Socket)
	end.

process(Tab, Socket, Src_IP, Src_Port, Packet) ->
	Data = case ?B2T(Packet) of
		{add, Key, Value} ->
			ets:insert(Tab, {Key, Value});
		{get, Key} ->
			ets:lookup(Tab, Key)
	end,
	BackPacket = ?T2B(Data),
	ok = gen_udp:send(Socket, Src_IP, Src_Port, BackPacket).


add(K, V) ->
	add(?IP, K, V).

add(IP, K, V) ->
	Data = {add, K, ?T2B_CLIENT(V)},
	client(IP, Data).


async_add(K, V) ->
	async_add(?IP, K, V).

async_add(IP, K, V) ->
	Socket = case erlang:get(sim_socket) of
		undefined ->
			{ok, Socket1} = gen_udp:open(0, [binary, {recbuf, 4096}, {reuseaddr, true}]), % default is 8192
			erlang:put(sim_socket, Socket1),
			Socket1;
		Socket1 -> Socket1
	end,
	Data = {add, K, ?T2B_CLIENT(V)},
	Port = ?PORT,
	Packet = ?T2B(Data),
	ok = gen_udp:send(Socket, IP, Port, Packet).



get(K) ->
	get(?IP, K).

get(IP, K) ->
	case client(IP, {get, K}) of
		[{_,V}] ->
			{value, ?B2T(V)};
		[] ->
			none
	end.

client(Op) ->
	client(?IP,Op).

client(IP, Op) ->
	client(IP, ?PORT, Op).

client(IP, Port, Data) ->
	Socket = case erlang:get(sim_socket) of
		undefined ->
			{ok, Socket1} = gen_udp:open(0, [binary, {recbuf, 4096}, {reuseaddr, true}]), % default is 8192
			erlang:put(sim_socket, Socket1),
			Socket1;
		Socket1 -> Socket1
	end,
	client(Socket, IP, Port, Data).

client(Socket, IP, Port, Data) ->
	Packet = ?T2B(Data),
	ok = gen_udp:send(Socket, IP, Port, Packet),
	receive
		{udp, Socket, IP, Port, BackPacket} ->
			?B2T(BackPacket)
		after 10000 ->
			throw(timeout)
	end.
