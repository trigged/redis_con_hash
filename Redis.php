<?php
/**
 * @method connect
 * @method open
 * @method pconnect
 * @method popen
 * @method close
 * @method setOption
 * @method getOption
 * @method ping
 * @method echo
 * @method get
 * @method set
 * @method setex
 * @method psetex
 * @method setnx
 * @method del
 * @method delete
 * @method multi
 * @method exec
 * @method discard.
 * @method watch
 * @method unwatch
 * @method subscribe
 * @method publish
 * @method exists
 * @method incr
 * @method incrBy
 * @method incrByFloat
 * @method decr
 * @method decrBy
 * @method mGet
 * @method getMultiple
 * @method lPush
 * @method rPush
 * @method lPushx
 * @method rPushx
 * @method lPop
 * @method rPop
 * @method blPop
 * @method brPop
 * @method lSize
 * @method lIndex
 * @method lGet
 * @method lSet
 * @method lRange
 * @method lGetRange
 * @method lTrim
 * @method listTrim
 * @method lRem
 * @method lRemove
 * @method lInsert
 * @method lLen
 * @method sAdd
 * @method sRem
 * @method sRemove
 * @method sMove
 * @method sIsMember
 * @method sContains
 * @method sCard
 * @method sSize
 * @method sPop
 * @method sRandMember
 * @method sInter
 * @method sInterStore
 * @method sUnion
 * @method sUnionStore
 * @method sDiff
 * @method sDiffStore
 * @method sMembers
 * @method sGetMembers
 * @method getSet
 * @method randomKey
 * @method select
 * @method move
 * @method rename
 * @method renameKey
 * @method renameNx
 * @method setTimeout
 * @method expire
 * @method pexpire
 * @method expireAt
 * @method pexpireAt
 * @method keys
 * @method getKeys
 * @method dbSize
 * @method auth
 * @method bgrewriteaof
 * @method slaveof
 * @method object
 * @method save
 * @method bgsave
 * @method lastSave
 * @method type
 * @method append
 * @method getRange
 * @method setRange
 * @method strlen
 * @method getBit
 * @method setBit
 * @method bitop
 * @method bitcount
 * @method flushDB
 * @method flushAll
 * @method sort
 * @method info
 * @method resetStat
 * @method ttl
 * @method pttl
 * @method persist
 * @method mset
 * @method msetnx
 * @method rpoplpush
 * @method brpoplpush
 * @method zAdd
 * @method zRange
 * @method zDelete
 * @method zRem
 * @method zRevRange
 * @method zRangeByScore
 * @method zRevRangeByScore
 * @method zCount
 * @method zRemRangeByScore
 * @method zDeleteRangeByScore
 * @method zRemRangeByRank
 * @method zDeleteRangeByRank
 * @method zSize
 * @method zCard
 * @method zScore
 * @method zRank
 * @method zRevRank
 * @method zIncrBy
 * @method zUnion
 * @method zInter
 * @method hSet
 * @method hSetNx
 * @method hGet
 * @method hLen
 * @method hDel
 * @method hKeys
 * @method hVals
 * @method hGetAll
 * @method hExists
 * @method hIncrBy
 * @method hIncrByFloat
 * @method hMset
 * @method hMGet
 * @method config
 * @method eval
 * @method evalSha
 * @method script
 * @method getLastError
 * @method _prefix
 * @method _unserialize
 * @method dump
 * @method restore
 * @method migrate
 * @method time
 */
class Redis
{
    const PIPELINE = \Redis::PIPELINE;
    const MULTI = \Redis::MULTI;

    protected static $_cache_commands = array(
        "getOption",
        "get",
        "multi",
        "exec",
        "exists",
        "mGet",
        "lSize",
        "lIndex",
        "lGet",
        "lRange",
        "lGetRange",
        "sIsMember",
        "sContains",
        "sCard",
        "sSize",
        "sInter",
        "sUnion",
        "sDiff",
        "sMembers",
        "sGetMembers",
        "keys",
        "getKeys",
        "getRange",
        "getBit",
        "zRange",
        "zRevRange",
        "zRangeByScore",
        "zRevRangeByScore",
        "zCount",
        "zSize",
        "zCard",
        "zScore",
        "zRank",
        "zRevRank",
        "hGet",
        "hLen",
        "hKeys",
        "hVals",
        "hGetAll",
        "hExists",
        "hMGet"
    );

    protected static $_caches = array();
    protected static $_cache = false;

    private $_redis;
    private $_in_multi = false;
    private $_multi_keys = array();
    private $_multi_used_keys = array();
    public $config;
    /**
     * Get redis instance with name
     *
     * @static
     * @param string $name      Config name
     * @param bool   $slave     If use slave mode?
     * @throws Exception
     * @return \Redis|Redis
     */
    public static function instance($name = 'default', $slave = false)
    {
        static $instance = array();
        // Set key for instance
        $key = $name . $slave;

        // Check instance or construct new instance
        if (!isset($instance[$key])) {
            if (!$slave) {
                if (!$config = Config::get('redis.' . $name)) {
                    throw new Exception("Config->redis.$name not exists");
                }
            } else {
                if (!$config = Config::get('redis')) {
                    throw new Exception("Config->redis not exists");
                }

                foreach ($config as $k => $item) {
                    // Remove config which is not start with "<name>"
                    if (substr($k, 0, strlen($name)) != $name) {
                        unset($config[$k]);
                    }
                }

                $index = array_rand($config);
                $config = $config[$index];
            }
            $instance[$key] = new self($config);
        }
        return $instance[$key];
    }

    /**
     * Construct and get redis
     */
    public function __construct($config)
    {
        $this->config = $config;
        $this->_redis = new \Redis();
        $this->_redis->connect($config['host'], $config['port'], 1);
        if ($config['db']) $this->_redis->select($config['db']);
    }

    /**
     * Magic call
     *
     * @param $method
     * @param $arguments
     * @return array|mixed|null|\Redis
     */
    public function __call($method, $arguments)
    {
        // On multi start
        if ($method == 'multi') {
            // Set multi flag
            $this->_in_multi = true;
            // Call redis
            return call_user_func_array(array($this->_redis, $method), $arguments);
            // On exec
        } elseif ($this->_in_multi && $method == 'exec') {
            // Call exec to get result
            $_caches = call_user_func_array(array($this->_redis, $method), $arguments);

            // Make empty results
            $_results = array();

            // Iterate the multi keys list
            foreach ($this->_multi_keys as $_key) {
                $_item = null;
                if (static::$_cache && isset(static::$_caches[$_key])) {
                    // Use cache
                    $_item = static::$_caches[$_key];
                } elseif (($pos = array_search($_key, $this->_multi_used_keys)) !== false) {
                    // Set key cache
                    $_item = $_caches[$pos];

                    $_method = substr($_key, 0, strpos($_key, '::'));

                    // Try to parse
                    $this->outputProxy($_method, $_item);

                    if (static::$_cache && in_array($_method, self::$_cache_commands) && !isset(static::$_caches[$_key])) {
                        static::$_caches[$_key] = $_item;
                    }
                }
                // Fill results
                $_results[] = $_item;
            }
            // Set empty multi keys
            $this->_multi_keys = array();
            // Set empty multi used to exec keys
            $this->_multi_used_keys = array();
            // Clear in multi flat
            $this->_in_multi = false;
            return $_results;
            // If in multi call
        } elseif ($this->_in_multi) {
            // Make key
            $key = $method . '::' . json_encode($arguments);

            // Set default result to false
            // Not in cache and not used to executed
            if ((!static::$_cache || !isset(static::$_caches[$key])) && !in_array($key, $this->_multi_used_keys)) {
                // Call build
                $this->inputProxy($method, $arguments);
                // Call redis
                call_user_func_array(array($this->_redis, $method), $arguments);
                // Push to used key list
                $this->_multi_used_keys[] = $key;
            }
            // Push to key list
            $this->_multi_keys[] = $key;

            // If not in multi, and no cache command
            return $this->_redis;
        } else {
            // Make key
            $key = $method . '::' . json_encode($arguments);

            // Check cache and set
            if (!static::$_cache || !isset(static::$_caches[$key])) {
                $this->inputProxy($method, $arguments);
                // Call redis and set to cache
                $_item = call_user_func_array(array($this->_redis, $method), $arguments);
                $this->outputProxy($method, $_item);

                if (static::$_cache && in_array($method, self::$_cache_commands) && !isset(static::$_caches[$key])) {
                    static::$_caches[$key] = $_item;
                }

                return $_item;
            }

            // return cache
            return isset(static::$_caches[$key]) ? static::$_caches[$key] : null;
        }
    }

    /**
     * Input proxy
     *
     * @param $method
     * @param $arguments
     */
    protected function inputProxy($method, &$arguments)
    {
        if (method_exists($this, $method . 'Input')) {
            $arguments = call_user_func_array(array($this, $method . 'Input'), $arguments);
        }
    }

    /**
     * Output proxy
     *
     * @param $method
     * @param $result
     */
    protected function outputProxy($method, &$result)
    {
        if (method_exists($this, $method . 'Output')) {
            $result = call_user_func(array($this, $method . 'Output'), $result);
        }
    }

    /**
     * hSet input build
     *
     * @param $key
     * @param $field
     * @param $value
     * @return array
     */
    protected function hSetInput($key, $field, $value)
    {
        $this->jsonBuild($value);
        return array($key, $field, $value);
    }

    /**
     * HGet input proxy
     *
     * @param $value
     * @return mixed
     */
    protected function hGetOutput($value)
    {
        $this->jsonParse($value);
        return $value;
    }

    /**
     * hMGet output proxy
     *
     * @param $value
     * @return mixed
     */
    protected function hMGetOutput($value)
    {
        foreach ($value as &$v) {
            $this->jsonParse($v);
        }
        return $value;
    }

    /**
     * hMSet input proxy
     *
     * @param $key
     * @param $value
     * @return array
     */
    protected function hMSetInput($key, $value)
    {
        foreach ($value as &$v) {
            $this->jsonBuild($v);
        }
        return array($key, $value);
    }

    /**
     * hGetAll output proxy
     *
     * @param $value
     * @return mixed
     */
    protected function hGetAllOutput($value)
    {
        foreach ($value as &$v) {
            $this->jsonParse($v);
        }
        return $value;
    }

    /**
     * Build json
     *
     * @param $value
     * @return void
     */
    protected function jsonBuild(&$value)
    {
        if (is_array($value)) {
            $value = 'j:' . json_encode($value);
        }
    }

    /**
     * Json parse
     *
     * @param $value
     */
    protected function jsonParse(&$value)
    {
        if (substr($value, 0, 2) == 'j:' && ($_json = json_decode(substr($value, 2), true))) {
            $value = $_json;
        }
    }

    /**
     * Enable cache
     */
    public static function enableCache()
    {
        static::$_cache = true;
    }

    /**
     * Disable cache
     */
    public static function disableCache()
    {
        static::$_cache = false;
        static::$_caches = array();
    }

    public function __destruct()
    {
        try {
            $this->close();
        } catch (\Exception $e) {
        }
    }
}