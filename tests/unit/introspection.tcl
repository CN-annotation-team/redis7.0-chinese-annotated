start_server {tags {"introspection"}} {
    test "PING" {
        assert_equal {PONG} [r ping]
        assert_equal {redis} [r ping redis]
        assert_error {*wrong number of arguments for 'ping' command} {r ping hello redis}
    }

    test {CLIENT LIST} {
        r client list
    } {id=* addr=*:* laddr=*:* fd=* name=* age=* idle=* flags=N db=* sub=0 psub=0 multi=-1 qbuf=26 qbuf-free=* argv-mem=* multi-mem=0 rbs=* rbp=* obl=0 oll=0 omem=0 tot-mem=* events=r cmd=client|list user=* redir=-1 resp=2*}

    test {CLIENT LIST with IDs} {
        set myid [r client id]
        set cl [split [r client list id $myid] "\r\n"]
        assert_match "id=$myid * cmd=client|list *" [lindex $cl 0]
    }

    test {CLIENT INFO} {
        r client info
    } {id=* addr=*:* laddr=*:* fd=* name=* age=* idle=* flags=N db=* sub=0 psub=0 multi=-1 qbuf=26 qbuf-free=* argv-mem=* multi-mem=0 rbs=* rbp=* obl=0 oll=0 omem=0 tot-mem=* events=r cmd=client|info user=* redir=-1 resp=2*}

    test {CLIENT KILL with illegal arguments} {
        assert_error "ERR wrong number of arguments for 'client|kill' command" {r client kill}
        assert_error "ERR syntax error*" {r client kill id 10 wrong_arg}

        assert_error "ERR *greater than 0*" {r client kill id str}
        assert_error "ERR *greater than 0*" {r client kill id -1}
        assert_error "ERR *greater than 0*" {r client kill id 0}

        assert_error "ERR Unknown client type*" {r client kill type wrong_type}

        assert_error "ERR No such user*" {r client kill user wrong_user}

        assert_error "ERR syntax error*" {r client kill skipme yes_or_no}
    }

    test {CLIENT KILL SKIPME YES/NO will kill all clients} {
        # Kill all clients except `me`
        set rd1 [redis_deferring_client]
        set rd2 [redis_deferring_client]
        set connected_clients [s connected_clients]
        assert {$connected_clients >= 3}
        set res [r client kill skipme yes]
        assert {$res == $connected_clients - 1}

        # Kill all clients, including `me`
        set rd3 [redis_deferring_client]
        set rd4 [redis_deferring_client]
        set connected_clients [s connected_clients]
        assert {$connected_clients == 3}
        set res [r client kill skipme no]
        assert_equal $res $connected_clients

        # After killing `me`, the first ping will throw an error
        assert_error "*I/O error*" {r ping}
        assert_equal "PONG" [r ping]
    }

    test {MONITOR can log executed commands} {
        set rd [redis_deferring_client]
        $rd monitor
        assert_match {*OK*} [$rd read]
        r set foo bar
        r get foo
        set res [list [$rd read] [$rd read]]
        $rd close
        set _ $res
    } {*"set" "foo"*"get" "foo"*}

    test {MONITOR can log commands issued by the scripting engine} {
        set rd [redis_deferring_client]
        $rd monitor
        $rd read ;# Discard the OK
        r eval {redis.call('set',KEYS[1],ARGV[1])} 1 foo bar
        assert_match {*eval*} [$rd read]
        assert_match {*lua*"set"*"foo"*"bar"*} [$rd read]
        $rd close
    }

    test {MONITOR supports redacting command arguments} {
        set rd [redis_deferring_client]
        $rd monitor
        $rd read ; # Discard the OK

        r migrate [srv 0 host] [srv 0 port] key 9 5000
        r migrate [srv 0 host] [srv 0 port] key 9 5000 AUTH user
        r migrate [srv 0 host] [srv 0 port] key 9 5000 AUTH2 user password
        catch {r auth not-real} _
        catch {r auth not-real not-a-password} _
        catch {r hello 2 AUTH not-real not-a-password} _

        assert_match {*"key"*"9"*"5000"*} [$rd read]
        assert_match {*"key"*"9"*"5000"*"(redacted)"*} [$rd read]
        assert_match {*"key"*"9"*"5000"*"(redacted)"*"(redacted)"*} [$rd read]
        assert_match {*"auth"*"(redacted)"*} [$rd read]
        assert_match {*"auth"*"(redacted)"*"(redacted)"*} [$rd read]
        assert_match {*"hello"*"2"*"AUTH"*"(redacted)"*"(redacted)"*} [$rd read]
        $rd close
    } {0} {needs:repl}

    test {MONITOR correctly handles multi-exec cases} {
        set rd [redis_deferring_client]
        $rd monitor
        $rd read ; # Discard the OK

        # Make sure multi-exec statements are ordered
        # correctly
        r multi
        r set foo bar
        r exec
        assert_match {*"multi"*} [$rd read]
        assert_match {*"set"*"foo"*"bar"*} [$rd read]
        assert_match {*"exec"*} [$rd read]

        # Make sure we close multi statements on errors
        r multi
        catch {r syntax error} _
        catch {r exec} _

        assert_match {*"multi"*} [$rd read]
        assert_match {*"exec"*} [$rd read]

        $rd close
    }

    test {CLIENT GETNAME should return NIL if name is not assigned} {
        r client getname
    } {}

    test {CLIENT LIST shows empty fields for unassigned names} {
        r client list
    } {*name= *}

    test {CLIENT SETNAME does not accept spaces} {
        catch {r client setname "foo bar"} e
        set e
    } {ERR*}

    test {CLIENT SETNAME can assign a name to this connection} {
        assert_equal [r client setname myname] {OK}
        r client list
    } {*name=myname*}

    test {CLIENT SETNAME can change the name of an existing connection} {
        assert_equal [r client setname someothername] {OK}
        r client list
    } {*name=someothername*}

    test {After CLIENT SETNAME, connection can still be closed} {
        set rd [redis_deferring_client]
        $rd client setname foobar
        assert_equal [$rd read] "OK"
        assert_match {*foobar*} [r client list]
        $rd close
        # Now the client should no longer be listed
        wait_for_condition 50 100 {
            [string match {*foobar*} [r client list]] == 0
        } else {
            fail "Client still listed in CLIENT LIST after SETNAME."
        }
    }

    test {CONFIG save params special case handled properly} {
        # No "save" keyword - defaults should apply
        start_server {config "minimal.conf"} {
            assert_match [r config get save] {save {3600 1 300 100 60 10000}}
        }

        # First "save" keyword overrides defaults
        start_server {config "minimal.conf" overrides {save {100 100}}} {
            # Defaults
            assert_match [r config get save] {save {100 100}}
        }
    } {} {external:skip}

    test {CONFIG sanity} {
        # Do CONFIG GET, CONFIG SET and then CONFIG GET again
        # Skip immutable configs, one with no get, and other complicated configs
        set skip_configs {
            rdbchecksum
            daemonize
            io-threads-do-reads
            tcp-backlog
            always-show-logo
            syslog-enabled
            cluster-enabled
            disable-thp
            aclfile
            unixsocket
            pidfile
            syslog-ident
            appendfilename
            appenddirname
            supervised
            syslog-facility
            databases
            io-threads
            logfile
            unixsocketperm
            replicaof
            slaveof
            requirepass
            server_cpulist
            bio_cpulist
            aof_rewrite_cpulist
            bgsave_cpulist
            set-proc-title
            cluster-config-file
            cluster-port
            oom-score-adj
            oom-score-adj-values
            enable-protected-configs
            enable-debug-command
            enable-module-command
            dbfilename
            logfile
            dir
            socket-mark-id
        }

        if {!$::tls} {
            append skip_configs {
                tls-prefer-server-ciphers
                tls-session-cache-timeout
                tls-session-cache-size
                tls-session-caching
                tls-cert-file
                tls-key-file
                tls-client-cert-file
                tls-client-key-file
                tls-dh-params-file
                tls-ca-cert-file
                tls-ca-cert-dir
                tls-protocols
                tls-ciphers
                tls-ciphersuites
                tls-port
            }
        }

        set configs {}
        foreach {k v} [r config get *] {
            if {[lsearch $skip_configs $k] != -1} {
                continue
            }
            dict set configs $k $v
            # try to set the config to the same value it already has
            r config set $k $v
        }

        set newconfigs {}
        foreach {k v} [r config get *] {
            if {[lsearch $skip_configs $k] != -1} {
                continue
            }
            dict set newconfigs $k $v
        }

        dict for {k v} $configs {
            set vv [dict get $newconfigs $k]
            if {$v != $vv} {
                fail "config $k mismatch, expecting $v but got $vv"
            }

        }
    }

    # Do a force-all config rewrite and make sure we're able to parse
    # it.
    test {CONFIG REWRITE sanity} {
        # Capture state of config before
        set configs {}
        foreach {k v} [r config get *] {
            dict set configs $k $v
        }

        # Rewrite entire configuration, restart and confirm the
        # server is able to parse it and start.
        assert_equal [r debug config-rewrite-force-all] "OK"
        restart_server 0 true false
        wait_done_loading r

        # Verify no changes were introduced
        dict for {k v} $configs {
            assert_equal $v [lindex [r config get $k] 1]
        }
    } {} {external:skip}

    test {CONFIG REWRITE handles save and shutdown properly} {
        r config set save "3600 1 300 100 60 10000"
        r config set shutdown-on-sigterm "nosave now"
        r config set shutdown-on-sigint "save"
        r config rewrite
        restart_server 0 true false
        assert_equal [r config get save] {save {3600 1 300 100 60 10000}}
        assert_equal [r config get shutdown-on-sigterm] {shutdown-on-sigterm {nosave now}}
        assert_equal [r config get shutdown-on-sigint] {shutdown-on-sigint save}

        r config set save ""
        r config set shutdown-on-sigterm "default"
        r config rewrite
        restart_server 0 true false
        assert_equal [r config get save] {save {}}
        assert_equal [r config get shutdown-on-sigterm] {shutdown-on-sigterm default}

        start_server {config "minimal.conf"} {
            assert_equal [r config get save] {save {3600 1 300 100 60 10000}}
            r config set save ""
            r config rewrite
            restart_server 0 true false
            assert_equal [r config get save] {save {}}
        }
    } {} {external:skip}
    
    test {CONFIG SET with multiple args} {
        set some_configs {maxmemory 10000001 repl-backlog-size 10000002 save {3000 5}}

        # Backup
        set backups {}
        foreach c [dict keys $some_configs] {
            lappend backups $c [lindex [r config get $c] 1]
        }

        # multi config set and veirfy
        assert_equal [eval "r config set $some_configs"] "OK"
        dict for {c val} $some_configs {
            assert_equal [lindex [r config get $c] 1] $val
        }

        # Restore backup
        assert_equal [eval "r config set $backups"] "OK"
    }

    test {CONFIG SET rollback on set error} {
        # This test passes an invalid percent value to maxmemory-clients which should cause an
        # input verification failure during the "set" phase before trying to apply the 
        # configuration. We want to make sure the correct failure happens and everything
        # is rolled back.
        # backup maxmemory config
        set mm_backup [lindex [r config get maxmemory] 1]
        set mmc_backup [lindex [r config get maxmemory-clients] 1]
        set qbl_backup [lindex [r config get client-query-buffer-limit] 1]
        # Set some value to maxmemory
        assert_equal [r config set maxmemory 10000002] "OK"
        # Set another value to maxmeory together with another invalid config
        assert_error "ERR CONFIG SET failed (possibly related to argument 'maxmemory-clients') - percentage argument must be less or equal to 100" {
            r config set maxmemory 10000001 maxmemory-clients 200% client-query-buffer-limit invalid
        }
        # Validate we rolled back to original values
        assert_equal [lindex [r config get maxmemory] 1] 10000002
        assert_equal [lindex [r config get maxmemory-clients] 1] $mmc_backup
        assert_equal [lindex [r config get client-query-buffer-limit] 1] $qbl_backup
        # Make sure we revert back to the previous maxmemory
        assert_equal [r config set maxmemory $mm_backup] "OK"
    }

    test {CONFIG SET rollback on apply error} {
        # This test tries to configure a used port number in redis. This is expected
        # to pass the `CONFIG SET` validity checking implementation but fail on 
        # actual "apply" of the setting. This will validate that after an "apply"
        # failure we rollback to the previous values.
        proc dummy_accept {chan addr port} {}

        set some_configs {maxmemory 10000001 port 0 client-query-buffer-limit 10m}

        # On Linux we also set the oom score adj which has an apply function. This is
        # used to verify that even successful applies are rolled back if some other
        # config's apply fails.
        set oom_adj_avail [expr {!$::external && [exec uname] == "Linux"}]
        if {$oom_adj_avail} {
            proc get_oom_score_adj {} {
                set pid [srv 0 pid]
                set fd [open "/proc/$pid/oom_score_adj" "r"]
                set val [gets $fd]
                close $fd
                return $val
            }
            set some_configs [linsert $some_configs 0 oom-score-adj yes oom-score-adj-values {1 1 1}]
            set read_oom_adj [get_oom_score_adj]
        }

        # Backup
        set backups {}
        foreach c [dict keys $some_configs] {
            lappend backups $c [lindex [r config get $c] 1]
        }

        set used_port [find_available_port $::baseport $::portcount]
        dict set some_configs port $used_port

        # Run a dummy server on used_port so we know we can't configure redis to 
        # use it. It's ok for this to fail because that means used_port is invalid 
        # anyway
        catch {socket -server dummy_accept -myaddr 127.0.0.1 $used_port} e
        if {$::verbose} { puts "dummy_accept: $e" }

        # Try to listen on the used port, pass some more configs to make sure the
        # returned failure message is for the first bad config and everything is rolled back.
        assert_error "ERR CONFIG SET failed (possibly related to argument 'port') - Unable to listen on this port*" {
            eval "r config set $some_configs"
        }

        # Make sure we reverted back to previous configs
        dict for {conf val} $backups {
            assert_equal [lindex [r config get $conf] 1] $val
        }

        if {$oom_adj_avail} {
            assert_equal [get_oom_score_adj] $read_oom_adj
        }

        # Make sure we can still communicate with the server (on the original port)
        set r1 [redis_client]
        assert_equal [$r1 ping] "PONG"
        $r1 close
    }

    test {CONFIG SET duplicate configs} {
        assert_error "ERR *duplicate*" {r config set maxmemory 10000001 maxmemory 10000002}
    }

    test {CONFIG SET set immutable} {
        assert_error "ERR *immutable*" {r config set daemonize yes}
    }

    test {CONFIG GET hidden configs} {
        set hidden_config "key-load-delay"

        # When we use a pattern we shouldn't get the hidden config
        assert {![dict exists [r config get *] $hidden_config]}

        # When we explicitly request the hidden config we should get it
        assert {[dict exists [r config get $hidden_config] "$hidden_config"]}
    }

    test {CONFIG GET multiple args} {
        set res [r config get maxmemory maxmemory* bind *of]
        
        # Verify there are no duplicates in the result
        assert_equal [expr [llength [dict keys $res]]*2] [llength $res]
        
        # Verify we got both name and alias in result
        assert {[dict exists $res slaveof] && [dict exists $res replicaof]}  

        # Verify pattern found multiple maxmemory* configs
        assert {[dict exists $res maxmemory] && [dict exists $res maxmemory-samples] && [dict exists $res maxmemory-clients]}  

        # Verify we also got the explicit config
        assert {[dict exists $res bind]}  
    }

    # Config file at this point is at a weird state, and includes all
    # known keywords. Might be a good idea to avoid adding tests here.
}

start_server {tags {"introspection external:skip"} overrides {enable-protected-configs {no} enable-debug-command {no}}} {
    test {cannot modify protected configuration - no} {
        assert_error "ERR *protected*" {r config set dir somedir}
        assert_error "ERR *DEBUG command not allowed*" {r DEBUG HELP}
    } {} {needs:debug}
}

start_server {config "minimal.conf" tags {"introspection external:skip"} overrides {protected-mode {no} enable-protected-configs {local} enable-debug-command {local}}} {
    test {cannot modify protected configuration - local} {
        # verify that for local connection it doesn't error
        r config set dbfilename somename
        r DEBUG HELP

        # Get a non-loopback address of this instance for this test.
        set myaddr [get_nonloopback_addr]
        if {$myaddr != "" && ![string match {127.*} $myaddr]} {
            # Non-loopback client should fail
            set r2 [get_nonloopback_client]
            assert_error "ERR *protected*" {$r2 config set dir somedir}
            assert_error "ERR *DEBUG command not allowed*" {$r2 DEBUG HELP}
        }
    } {} {needs:debug}
}

test {config during loading} {
    start_server [list overrides [list key-load-delay 50 loading-process-events-interval-bytes 1024 rdbcompression no]] {
        # create a big rdb that will take long to load. it is important
        # for keys to be big since the server processes events only once in 2mb.
        # 100mb of rdb, 100k keys will load in more than 5 seconds
        r debug populate 100000 key 1000

        restart_server 0 false false

        # make sure it's still loading
        assert_equal [s loading] 1

        # verify some configs are allowed during loading
        r config set loglevel debug
        assert_equal [lindex [r config get loglevel] 1] debug

        # verify some configs are forbidden during loading
        assert_error {LOADING*} {r config set dir asdf}

        # make sure it's still loading
        assert_equal [s loading] 1

        # no need to keep waiting for loading to complete
        exec kill [srv 0 pid]
    }
} {} {external:skip}
