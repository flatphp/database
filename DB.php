<?php namespace Flatphp\Database;

class DB
{
    protected static $_config;
    /**
     * @var Connection
     */
    protected static $_connection;
    /**
     * @var Sql\Builder
     */
    protected static $_last_query;


    /**
     * init with config array or Connection instance
     * @param array|Connection $conn
     */
    public static function init($conn)
    {
        if ($conn instanceof Connection) {
            self::$_connection = $conn;
        } elseif (is_array($conn)) {
            self::$_config = $conn;
        } else {
            throw new \InvalidArgumentException('Invalid DB init params');
        }
    }

    /**
     * @return Connection
     */
    public static function getConnection()
    {
        if (null === self::$_connection) {
            self::$_connection = new Connection(self::$_config);
        }
        return self::$_connection;
    }

    /**
     * @return Sql\BaseGrammar|null
     */
    public static function getGrammar()
    {
        static $grammar = null;
        if (null === $grammar) {
            $grammar = new Sql\BaseGrammar();
        }
        return $grammar;
    }

    /**
     * @param null|string $table
     * @return Sql\Builder
     */
    public static function query($table = null)
    {
        $query = new Sql\Builder(self::getGrammar(), self::getConnection());
        if ($table) {
            $query = $query->table($table);
        }
        self::$_last_query = $query;
        return $query;
    }

    /**
     * @param $table
     * @param $data
     * @return bool
     */
    public static function insert($table, $data)
    {
        return self::query($table)->insert($data)->execute();
    }

    /**
     * @param $table
     * @param $data
     * @param $where
     * @return bool
     */
    public static function update($table, $data, $where)
    {
        return self::query($table)->where($where)->update($data)->execute();
    }

    /**
     * @param $table
     * @param $where
     * @return bool
     */
    public static function delete($table, $where)
    {
        return self::query($table)->where($where)->delete()->execute();
    }

    /**
     * Get last query log
     * @return array
     */
    public static function getLastLog()
    {
        return array(
            'sql' => self::$_last_query->getSql(),
            'bind' => self::$_last_query->getBind()
        );
    }
    

    /**
     * @param string $method
     * @param array $args
     * @return mixed
     */
    public static function __callStatic($method, $args = [])
    {
        return call_user_func_array([self::getConnection(), $method], $args);
    }
}
