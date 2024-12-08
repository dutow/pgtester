[3000].each do |scale|
    [0,1,2].each do |do_encrypt|
        clients = (1..5).to_a.join(',')
            
        puts ">> #{do_encrypt} #{scale} #{clients} #{ARGV[0]}"
        `ruby pgtest.rb #{do_encrypt} #{scale} #{clients} #{ARGV[0]}`
        sleep 1
    end
end