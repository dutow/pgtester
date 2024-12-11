
# bundle install!
require 'sys-cpu'

DO_ENCRYPT=ARGV[0].to_i != 0
DO_WAL_ENCRYPT=ARGV[0].to_i == 2
SCALING=ARGV[1]
CLIENTS=ARGV[2]
TIME=60
WARMUP_TIME=2 * 60
OUTPUT_FILE=ARGV[3]
LOAD_THRESHOLD=0.2

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

class Output

    def log message
        STDERR.puts message
    end
    
    def log_cmd name, status, params, stdout, stderr
        log ">>>> #{name} #{status}#{ " | " + params.join(",") unless params.empty?}"
        log "===== STDOUT"
        log stdout
        log "===== STDERR"
        log stderr
    end

    def result message
        puts message
    end

end

class Helper

    def initialize(output)
        @output = output
    end

    def wait_for_no_load
        @output.log ">>>> wait_for_load 0 | 0.2"
        loop do
            load = Sys::CPU.load_avg

            @output.log load.map(&:to_s).join(',')

            break if load[0] <= LOAD_THRESHOLD

            sleep 1
        end
    end

    def exec_and_log cmd, *args
        @output.log ">>>> pgbench | #{args.join(' ')}"
        @output.log "===== OUTPUT"
        @outputs = { stdout: '', stderr: ''}
        Open3.popen3(cmd, *args)  do |stdin, stdout, stderr, wait_thr|
            threads = [[stdout, :stdout], [stderr, :stderr]].collect do |p|
                Thread.new do
                  @output.log "[[#{p[1]}]] THREAD STARTING"
                  begin
                  while ((line = p[0].gets) != nil) do
                    unless line.empty?
                        @output.log "[[#{p[1]}]] #{line}"
                        @outputs[p[1]] += line + "\n"
                    end
                  end
                    rescue => error
                        @output.log "[[#{p[1]}]] closed #{error.message}"
                    end
                  p[0].close
                  @output.log "[[#{p[1]}]] THREAD EXITING"
                end
            end

            threads.each(&:join)
        end

        return @outputs
    end
end

class PgHandler

    attr_accessor :datadir

    attr_accessor :config

    # main methods to use
    
    def startup
        cleanup
        do_initdb
        do_pgstart
        sleep 3
    end

    def shutdown
        do_pgstop
        #cleanup
    end

    def restart
        do_pgstop
        do_pgstart
    end

    def setup_encryption(dbname, encrypt_wal)
        do_encryptdb "testdb"
        do_psql "testdb", "ALTER SYSTEM SET default_table_access_method=tde_heap"
        if encrypt_wal
            do_psql "testdb", "ALTER SYSTEM SET pg_tde.wal_encrypt=ON"
        end
        restart    
    end

    # internal metods

    def initialize(output)
        @output = output
        @config = { shared_preload_libraries: "pg_tde", work_mem: "1GB", shared_buffers: "16GB", max_wal_size: 16384 }
    end

    def generate_pg_conf fields
        # We add everything to the end of the configuration - this always work, if a key is specified multiple times,
        # the last occurence wins
        @output.log ">>>> configure 0 | postgresql.conf"
        @output.log "===== CONFIG"
        File.open("#{datadir}/postgresql.conf", 'a') do |f| 
            str = fields.map{ |k,v| "#{k}=#{v}" }.join("\n")
            @output.log str
            f.puts(str)
        end
    end    

    def cleanup
        @output.log ">>>> rmdir 0 | #{datadir}"
        FileUtils.rm_rf datadir
    end   
    
    def do_initdb
        stdout, stderr, status = Open3.capture3(PG_ENV[:initdb], '-D', datadir)
        @output.log_cmd "initdb", status, [], stdout, stderr
        generate_pg_conf @config
        @logfile = File.expand_path(datadir) + "/postgres.log"
    end

    def do_createdb(name)
        stdout, stderr, status = Open3.capture3(PG_ENV[:createdb], name)
        @output.log_cmd "createdb", status, [name], stdout, stderr
    end

    def do_psql(dbname, sql)
        stdout, stderr, status = Open3.capture3(PG_ENV[:psql], dbname, "-c", sql)
        @output.log_cmd "psql", status, [sql], stdout, stderr
    end

    def do_encryptdb(name)
        do_psql name, "CREATE EXTENSION pg_tde;"
        do_psql name, "SELECT pg_tde_add_key_provider_file('file-store','/#{File.expand_path(datadir)}/pg_tde_test_keyring.per');"
        do_psql name, "SELECT pg_tde_set_principal_key('db-principal-key','file-store');"
    end

    def do_pgstart
        @output.log ">>>> pg_ctl 0 | start"
        Open3.popen3(PG_ENV[:pg_ctl], '-D', datadir, '-l', @logfile, 'start') do |stdin, stdout, stderr, wait_thr|
            @stdin = stdin
            @process_status = wait_thr.value #.exitstatus
            @threads = [stdout, stderr].collect do |output|
                Thread.new do
                  @output.log "<<PG_CTL>> THREAD STARTING"
                  begin
                  while ((line = output.gets) != nil) do
                    unless line.empty?
                        @output.log "<<PG_CTL>> #{line}"
                    end
                  end
                    rescue => error
                        @output.log "<<PG_CTL>> closed #{error.message}"
                    end
                  output.close
                  @output.log "<<PG_CTL>> THREAD EXITING"
                end
            end
        end
    end
    
    def do_pgstop
        stdout, stderr, status = Open3.capture3(PG_ENV[:pg_ctl], '-D', datadir, 'stop')
        @output.log_cmd "pg_ctl", status, ['stop'], stdout, stderr

        @stdin.close
        @threads.each(&:join)

        puts "===== PG_LOG"
        puts File.read(@logfile)
    end

end

class PgBench

    def initialize(output, helper)
        @output = output
        @helper =  helper
    end

    def setup(dbname, scale)
        @helper.exec_and_log  PG_ENV[:pgbench], '-i', '-s', scale.to_s, dbname
    end

    def run(dbname, clients, time)
        @helper.wait_for_no_load

        @outputs = @helper.exec_and_log PG_ENV[:pgbench],  '-c', clients.to_s, '-C', '-P', '5', '-T', time.to_s, dbname

        return @outputs[:stdout][/tps = [0-9]+\.[0-9]+/,0].split(' = ')[1]
    end
end

out = Output.new

helper = Helper.new(out)

pg = PgHandler.new(out)
pg.datadir = DATADIR

pg.startup
pg.do_createdb "testdb"
if DO_ENCRYPT
    pg.setup_encryption("testdb", DO_WAL_ENCRYPT)
end

bench = PgBench.new(out, helper)
bench.setup "testdb", SCALING

CLIENTS.split(',').each do |clients|
    tps = bench.run "testdb", clients, TIME
    unless OUTPUT_FILE.nil?
        File.open(OUTPUT_FILE, 'a') do |f|
            out.result [DO_ENCRYPT, DO_WAL_ENCRYPT, "pgbench", SCALING, clients, TIME, tps].join(';')
        end
    end
end

pg.shutdown