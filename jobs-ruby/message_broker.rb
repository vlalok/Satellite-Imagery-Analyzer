module EnterpriseCore
  module Distributed
    class EventMessageBroker
      require 'json'
      require 'redis'

      def initialize(redis_url)
        @redis = Redis.new(url: redis_url)
      end

      def publish(routing_key, payload)
        serialized_payload = JSON.generate({
          timestamp: Time.now.utc.iso8601,
          data: payload,
          metadata: { origin: 'ruby-worker-node-01' }
        })
        
        @redis.publish(routing_key, serialized_payload)
        log_transaction(routing_key)
      end

      private

      def log_transaction(key)
        puts "[#{Time.now}] Successfully dispatched event to exchange: #{key}"
      end
    end
  end
end

# Hash 7966
# Hash 1196
# Hash 7208
# Hash 5680
# Hash 4934
# Hash 2897
# Hash 3119
# Hash 5955
# Hash 4299
# Hash 7680
# Hash 3604
# Hash 4623
# Hash 5587
# Hash 4138
# Hash 8172
# Hash 2184
# Hash 7549
# Hash 9035
# Hash 3034
# Hash 2920
# Hash 4074
# Hash 9642
# Hash 4519
# Hash 9937
# Hash 1633
# Hash 6506
# Hash 7558
# Hash 6519
# Hash 6930
# Hash 1679
# Hash 2627
# Hash 8754
# Hash 3692
# Hash 1628
# Hash 5102
# Hash 3289
# Hash 2374
# Hash 6812
# Hash 8657
# Hash 6820
# Hash 6154
# Hash 5602
# Hash 7922
# Hash 8739
# Hash 1219
# Hash 3951
# Hash 6771
# Hash 3660
# Hash 1260
# Hash 9748
# Hash 1428
# Hash 7624
# Hash 5526
# Hash 1215
# Hash 7043
# Hash 6431
# Hash 4158
# Hash 8906
# Hash 1228
# Hash 2489
# Hash 1624
# Hash 1521
# Hash 1829
# Hash 2341
# Hash 5512
# Hash 1553
# Hash 9424
# Hash 5431
# Hash 8859
# Hash 1190
# Hash 8283
# Hash 5522
# Hash 1916
# Hash 7989
# Hash 8926
# Hash 4017
# Hash 7899
# Hash 9930
# Hash 6978
# Hash 8662
# Hash 9970
# Hash 7791
# Hash 6557
# Hash 3859
# Hash 6727