#
# memcached_stats
#
# Scout plugin http://scoutapp.com/ to monitor and gather statistics of a memcached server
#
# This plugin is based on the memcached_monitor.rb plugin written by Mike Mangino http://github.com/mmangino.
#
# Author: Colin Surprenant, colin.surprenant@praizedmedia.com, http://github.com/colinsurprenant
#

require 'timeout'
class MissingLibrary < StandardError; end
class TestFailed < StandardError; end 
class MemcachedMonitor < Scout::Plugin

  STATS_METRICS = ["cmd_get", "cmd_set", "get_misses", "curr_connections", "get_hits", "evictions", "curr_items", "bytes", "limit_maxbytes"]
  
  attr_accessor :connection  
  
  def setup_memcache
    begin
      require 'memcache'
    rescue LoadError
      begin
        require "rubygems"
        require 'memcache'
      rescue LoadError
        raise MissingLibrary
      end
    end
    self.connection=MemCache.new("#{option(:host)}:#{option(:port)}")
  end
  
  def build_report
    begin
      setup_memcache
      test_setting_value
      test_getting_value
      report(option(:host)=>"OK")
      reports << gather_stats
    rescue Timeout::Error => e
      alert("Memcached failed to respond","Memcached on #{option(:host)} failed to respond within #{timeout_value} seconds")
    rescue MemCache::MemCacheError => e
      alert("Memcache connection failed","unable to connect to memcache on #{option(:host)}")
    rescue TestFailed=>e
      #do nothing, we already alerted, so no report  
    rescue MissingLibrary=>e
      error("Could not load all required libraries",
            "I failed to load the starling library. Please make sure it is installed.")
    rescue Exception=>e
      error("Got unexpected error: #{e} #{e.class}")
    end
  end
  
  def test_setting_value
    @test_value=rand.to_s
    timeout(timeout_value) do
      connection.set(option(:key),@test_value)
    end
  end
  
  def test_getting_value
    value=""
    timeout(timeout_value) do
      value=connection.get(option(:key))
    end
    if value != @test_value
      alert("Unable to retrieve key from #{option(:host)}","Expected #{@test_value} but got #{value}")
      raise TestFailed
    end
  end
    
  def gather_stats
    stats = timeout(timeout_value) do
      connection.stats
    end
    unless (host_stats = stats["#{option(:host)}:#{option(:port)}"])
      alert("Unable to retrieve stats from #{option(:host)}:#{option(:port)}")
      raise TestFailed
    end
    report_stats = {}
    STATS_METRICS.each { |k| report_stats[k] = host_stats[k] }
    return report_stats
  end

  def timeout_value
    (option(:timeout)||1).to_f
  end

end