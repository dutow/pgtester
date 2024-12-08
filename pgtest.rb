

DO_ENCRYPT=ARGV[0].to_i != 0
DO_WAL_ENCRYPT=ARGV[0].to_i == 2
SCALING=ARGV[1]
CLIENTS=ARGV[2]
TIME=60
OUTPUT_FILE=ARGV[3]

require 'open3'
require 'fileutils'

PG_ENV = {
    initdb: '/home/dutow/work/pg17ir/bin/initdb',
    pg_ctl: '/home/dutow/work/pg17ir/bin/pg_ctl',
    pgbench: '/home/dutow/work/pg17ir/bin/pgbench',
    createdb: '/home/dutow/work/pg17ir/bin/createdb',
    psql: '/home/dutow/work/pg17ir/bin/psql',
}

DATADIR = 'pgdata'

class PgHandler

    attr_accessor :datadir

    attr_accessor :config

    # main methods to use
    
    def startup
        cleanup
        do_initdb
        #generate_pgconf
        do_pgstart
    end

    def shutdown
        do_pgstop
        #cleanup
    end

    # internal metods

    def initialize(*args)
        super(*args)
        @config = { shared_preload_libraries: "pg_tde", work_mem: "1GB", shared_buffers: "16GB", max_wal_size: 16384 }
    end

    def generate_pg_conf fields
        File.open("#{datadir}/postgresql.conf", 'a') do |f| 
            str = fields.map{ |k,v| "#{k}=#{v}" }.join("\n")
            puts str
            f.puts(str)
        end
    end    

    def cleanup
        puts " [CLEANUP]"
        FileUtils.rm_rf datadir
    end   
    
    def do_initdb
        stdout, stderr, status = Open3.capture3(PG_ENV[:initdb], '-D', datadir)
        generate_pg_conf @config
        puts "===== #{status}"
        puts stdout
        puts "====="
        puts stderr
    end

    def do_createdb(name)
        stdout, stderr, status = Open3.capture3(PG_ENV[:createdb], name)
        puts "===== #{status}"
        puts stdout
        puts "====="
        puts stderr
    end

    def do_psql(dbname, sql)
        stdout, stderr, status = Open3.capture3(PG_ENV[:psql], dbname, "-c", sql)
        puts "===== #{status} >> #{sql}"
        puts stdout
        puts "====="
        puts stderr
    end

    def do_encryptdb(name)
        do_psql name, "CREATE EXTENSION pg_tde;"
        do_psql name, "SELECT pg_tde_add_key_provider_file('file-store','/#{File.expand_path(datadir)}/pg_tde_test_keyring.per');"
        do_psql name, "SELECT pg_tde_set_principal_key('db-principal-key','file-store');"
    end

    def generate_pgconf
        pgconf = generate_pg_conf(@config)

        File.write(datadir + '/postgres.conf', pgconf)
    end

    def do_pgstart
        puts "Starting server"
        Open3.popen3(PG_ENV[:pg_ctl], '-D', datadir, 'start') do |stdin, stdout, stderr, wait_thr|
            @process_status = wait_thr.value #.exitstatus
            stdin.close
            @threads = [stdout, stderr].collect do |output|
                Thread.new do
                  begin
                  while ((line = output.gets) != nil) do
                    unless line.empty?
                      puts "PG_CTL :: #{line}"
                    end
                  end
                    rescue
                        puts "closed"
                    end
                  output.close
                  puts "THREAD EXITING"
                end
            end
        end
        puts "Started server"
    end
    
    def do_pgstop
        stdout, stderr, status = Open3.capture3(PG_ENV[:pg_ctl], '-D', datadir, 'stop')
        puts "END: ===== #{status}"
        puts stdout
        puts "END: ====="
        puts stderr

        @threads.each(&:join)
    end

end

class PgBench
    def setup(dbname, scale)
        stdout, stderr, status = Open3.capture3(PG_ENV[:pgbench], '-i', '-s', scale.to_s, dbname)
        puts "END: ===== #{status}"
        puts stdout
        puts "END: ====="
        puts stderr
    end

    def run(dbname, clients, time)
        stdout, stderr, status = Open3.capture3(PG_ENV[:pgbench], '-c', clients.to_s, '-T', time.to_s, dbname)
        puts "END: ===== #{status}"
        puts stdout
        puts "END: ====="
        puts stderr

        return stdout[/tps = [0-9]+\.[0-9]+/,0].split(' = ')[1]
    end
end

pg = PgHandler.new
pg.datadir = DATADIR

pg.startup

sleep 3

pg.do_createdb "testdb"


if DO_ENCRYPT
    pg.do_encryptdb "testdb"
    pg.do_psql "testdb", "ALTER SYSTEM SET default_table_access_method=tde_heap"
    if DO_WAL_ENCRYPT
        pg.do_psql "testdb", "ALTER SYSTEM SET pg_tde.wal_encrypt=ON"
    end
    pg.do_pgstop
    pg.do_pgstart

    sleep 3
end

bench = PgBench.new


bench.setup "testdb", SCALING

CLIENTS.split(',').each do |clients|
    tps = bench.run "testdb", clients, TIME
    unless OUTPUT_FILE.nil?
        File.open(OUTPUT_FILE, 'a') do |f|
            f.puts [DO_ENCRYPT, DO_WAL_ENCRYPT, SCALING, clients, TIME, tps].join(';')
        end
    end
end


pg.shutdown