-module(simplememcache_stress).
-export([go/0]).

go() ->
	MainSeed = erlang:md5(term_to_binary({self(),now(),make_ref()})),
	<<A:32,B:32,C:32,_/binary>> = MainSeed,
	random:seed(A, B, C),
	loop(100000).

loop(0) ->
	done;
loop(N) ->
	%simplememcache:fullasync_add(key(), value()),
	simplememcache:async_add(key(), value()),
	flush(),
	loop(N-1).


key() ->
	list_to_binary(
		[$k,$a] ++ integer_to_list(random:uniform(100000)) ++ integer_to_list(random:uniform(100000)) ++ integer_to_list(random:uniform(100000))
	).
value() ->
	lists:foldl(
		fun(X,Acc) ->
		E = {X,X, lists:seq(1, 5)},
		[E|Acc]
		end,
		[],
		lists:seq(1,5)
	).

flush() ->
	receive
		_ -> flush()
		after 0 -> ok
	end.
