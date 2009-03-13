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

  SIZE_METRICS = ["bytes", "limit_maxbytes", "bytes_read", "bytes_written"]
  VALUE_CHARS = ('a'..'z').to_a
  
  attr_accessor :connection  
  
  def setup_memcache
    begin
      require 'memcache'
    rescue LoadError
      begin
        require "rubygems"
        require 'memcache'
      rescue LoadError
        raise MissingLibrary, "could not load the memcache gem"
      end
    end
    self.connection = MemCache.new("#{option(:host)}:#{option(:port)}")
  end
  
  def build_report
    begin
      setup_memcache
      test_setting_value
      test_getting_value
      report(gather_stats)
    rescue MissingLibrary => e
      # the MissingLibrary rescue must be before the MemCache::MemCacheError rescue because
      # if the gem is not loaded, the exception class will not be defined either.
      error("missing library", e.message)
    rescue Timeout::Error => e
      alert("memcached timeout", "memcached on #{option(:host)}:#{option(:port)} failed to respond within #{timeout_value} seconds")
    rescue MemCache::MemCacheError => e
      alert("memcache connection failed", "unable to connect to memcached on #{option(:host)}:#{option(:port)}")
    rescue TestFailed => e
      alert(e.message)
    rescue Exception => e
      error("unexpected exception: #{e.class}, #{e.message}")
    end
  end

  def test_setting_value
    @test_value = (1..4).collect { |a| VALUE_CHARS[rand(VALUE_CHARS.size)] }.join
    timeout(timeout_value) do
      connection.set(option(:key), @test_value)
    end
  end

  def test_getting_value
    value = timeout(timeout_value) do
      connection.get(option(:key))
    end
    if value != @test_value
      raise TestFailed, "bad data from #{option(:host)}, expected #{@test_value} but got #{value}"
    end
  end

  def gather_stats
    stats = timeout(timeout_value) do
      connection.stats
    end
    unless (host_stats = stats["#{option(:host)}:#{option(:port)}"])
      raise TestFailed, "unable to retrieve stats from #{option(:host)}:#{option(:port)}"
    end
    report_stats = {}
    option(:metrics).split(/\s*,\s/).each { |k| report_stats[add_unit(k)] = host_stats[k] }
    SIZE_METRICS.each do |k|
      new_k = add_unit(k)
      report_stats[new_k] = cast_unit(report_stats[new_k], option(:units)) if report_stats.has_key?(new_k)
    end
    return report_stats
  end
    
  def add_unit(k)
    SIZE_METRICS.include?(k) ? "#{k}(#{option(:units)})" : k
  end
  
  def cast_unit(bytes, unit)
    case unit
      when "B"
        return bytes
      when "KB"
        return round_to(bytes / 1024, 2)
      when "MB" 
        return round_to(bytes / (1024 * 1024), 2)
      when "GB"
        return round_to(bytes / (1024 * 1024 * 1024), 2)
    end
  end

  def round_to(f, x)
    (f * 10**x).round.to_f / 10**x
  end

  def timeout_value
    (option(:timeout) || 1).to_f
  end

end