<?php
use \Redis;

class RedisHelper
{
    private $config = array(
        'api_1' => array(
            'host' => 'xxx',
            'port' => 'xxx',
            'db' => 'x',
        ),
        'api_2' => array(
            'host' => 'xxx',
            'port' => 'xxx',
            'db' => 'x',
        ), 'api_3' => array(
            'host' => 'xxx',
            'port' => 'xxx',
            'db' => 'x',
        ));

    private $servers = array();

    private $virtual = array();

    private $virtual_count = 50;

    private $_in_multi = false;
    public $count = 0;

    public static function instance()
    {
        static $instance = array();
        // Set key for instance
        $key = "redis";

        // Check instance or construct new instance
        if (!isset($instance[$key])) {
            $instance[$key] = new self($key);
        }
        return $instance[$key];
    }

    /**
     * Construct and get redis
     */
    public function __construct()
    {

        foreach ( $this->config  as $key => $config) {
            if ($key == "job") {
                continue;
            }
            $this->buildServer($config);
        }
        ksort($this->virtual);
    }

    public function __call($method, $arguments)
    {
        if (empty($method)) {
            echo "\r\n method name can't be null, arg :" . $method . "\r\n";
            return null;
        }
        //如是管道传输 则开启所有redis 同开同关
        if ($method == 'multi') {
            $this->_in_multi = true;
            foreach ($this->servers as $redis) {
                $redis = $redis["redis"];
                call_user_func_array(array($redis, $method), $arguments);
            }
            return null;
        } elseif ($this->_in_multi && $method == 'exec') {
            $result = array();
            foreach ($this->servers as $redis) {
                $redis = $redis["redis"];
                $result[] = call_user_func_array(array($redis, $method), $arguments);
            }
            $this->_in_multi = false;
            return $result;
        }
        $key = $arguments[0];
        $hash_value = $this->hash2($key);
        $last_key = end(array_keys($this->virtual));
        foreach ($this->virtual as $server_key => $server) {
            if ($hash_value < $server_key) {
                return call_user_func_array(array($server, $method), $arguments);
            }
            //如果没有匹配的key 则把元素放入最后一个
            if ($server_key == $last_key) {
                return call_user_func_array(array($server, $method), $arguments);
            }
        }


    }

    private function hash2($string)
    {
        return crc32($string);
    }

    private function buildServer($config, $needBalance = false)
    {
        $master_hash = $this->hash2(join("", $config));
        $redis = new Redis($config);
        $this->servers[$master_hash]["redis"] = $redis;
        //建立虚拟节点,链接关系
        for ($i = 0; $i <= $this->virtual_count; $i++) {
            $virtual_hash = $this->hash2(join("", $config) . $i);
            $this->servers[$master_hash]["virtual"][$virtual_hash] = null;
            $this->virtual[$virtual_hash] = $redis;
            if ($needBalance === true) {
                ksort($this->virtual);
                $this->addVirtual($virtual_hash);
            }
        }
    }

    /**
     * @param $redis
     * 把redis 上的数据迁移到其他服务器上
     */
    public function balanceRedis($redis)
    {
        if (empty($redis))
            return;
        $last_key = end(array_keys($this->virtual));
        foreach ($redis->keys('*') as $key) {
            $hash_value = $this->hash2($key);
            foreach ($this->virtual as $server_key => $server) {
                if ($hash_value < $server_key) {
                    $this->translate($key, $server_key, $redis, $server);
                    break;
                }
                //如果没有匹配的key 则把元素放入最后一个
                if ($server_key == $last_key) {
                    $this->translate($key, $server_key, $redis, $server);
                    break;
                }
            }
        }
    }

    public function addServer($config)
    {
        $hash_key = $this->hash2(join("", $config));
        if (array_key_exists($hash_key, $this->servers)) {
            echo "\r\n" . "server exists, config :" . join("", $config) . "hash :" . $hash_key . "\r\n";
            return;
        }
        $this->buildServer($config, true);
    }

    public function removeServer($config)
    {
        $hash_key = $this->hash2(join("", $config));
        if (!array_key_exists($hash_key, $this->servers)) {
            echo "\r\n" . "server not  exists, config  :" . join("", $config) . "hash :" . $hash_key . "\r\n";
            return;
        }
        $delete_redis = $this->servers[$hash_key]["redis"];
        //清空server 内的virtual 信息
        foreach (array_keys($this->servers[$hash_key]["virtual"]) as $virtual) {
            $delete_redis->delete($virtual);
            unset($this->virtual[$virtual]);
        }
        unset($this->servers[$hash_key]);
        $this->balanceRedis($delete_redis);
    }

    public function balanceVirtual($virtual)
    {
        if (empty($virtual) || !$oldServer = $this->virtual[$virtual])
            return;

        $last_key = end(array_keys($this->virtual));
        foreach ($oldServer->sMembers($virtual) as $key) {
            $hash_value = $this->hash2($key);
            //todo 找出virtual 的上一个元素和下一个元素进行迁移
            foreach ($this->virtual as $server_key => $server) {
                if ($hash_value < $server_key) {
                    $this->translate($key, $server_key, $oldServer, $server, true);
                    break;
                }
                //如果没有匹配的key 则把元素放入最后一个
                if ($server_key == $last_key) {
                    $this->translate($key, $server_key, $oldServer, $server, true);
                    break;
                }
            }
        }
    }

    /**
     * @param $virtual
     * 把virtual 上的key 迁移到前后的节点上
     */
    public function removeVirtual($virtual)
    {
        $this->balanceVirtual($virtual);
    }

    /**
     * @param $virtual
     * 把virtual前后上的key 平衡到3者上
     */
    public function addVirtual($virtual)
    {
        if (empty($virtual) || !$oldServer = $this->virtual[$virtual])
            return;
        $last_key = end(array_keys($this->virtual));
        $prev = null;
        $next = null;
        $curr = null;
        foreach (array_keys($this->virtual) as $key) {
            if (!empty($curr)) {
                $next = $key;
                break;
            }
            if ($key == $virtual) {
                $curr = $key;
            }
            if (empty($curr)) {
                $prev = $key;
            }
        }
        $this->balanceVirtual($prev);
        $this->balanceVirtual($next);
    }

    public function translate($key, $server_key, Redis $old_redis, Redis $new_redis, $debug = false)
    {
        try {
            if ($old_redis == $new_redis) {
                echo "\r\n" . $key . " :break, old config" . $old_redis->config . " new config :" . $new_redis->config . "\r\n";
                return true;
            }
            if ($debug === true) {
                $this->count += 1;
            }
            $type = $old_redis->type($key);
            if ($type == 0) { //not exists\
                echo "\r\n" . $key . "not exists, server_key " . $server_key . " old config" . $old_redis->config . "\r\n";
                return false;
            } elseif ($type == 1) { // string
                $new_redis->set($key, $old_redis->get($key));
//            $old_redis->del($key);
            } elseif ($type == 2) { //set
                foreach ($old_redis->SMEMBERS($key) as $value) {
                    $new_redis->sadd($key, $value);
                }
//            $old_redis->del($key);
//            return true;
            } elseif ($type == 3) { //list
                $new_redis->rpush($key, $old_redis->lrange($key, 0, -1));
//            $old_redis->del($key);
//            return true;
            } elseif ($type == 4) { //zset
                foreach ($old_redis->zrange($key, 0, -1, true) as $score => $value) {
                    $new_redis->zAdd($key, $value, $score);
                }
//            $old_redis->del($key);
//            return true;
            } elseif ($type == 5) { //hash
//                echo "\r\n" . $key . "\r\n";
                $new_redis->hmset($key, $old_redis->hgetall($key));
//            $old_redis->del($key);
//            return true;
            }
            //数据迁移到新的redis 之后需要记录virtual 上有哪些key
            $new_redis->sAdd($server_key, $key);
            if ($debug) {
                $old_redis->sRemove($server_key, $key);
            }
        } catch (Exception $e) {
            var_dump($e);
        }
        //@todo dump exception info
    }
}

?>