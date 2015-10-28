package main

import (
	"time"
    "testing"
	"github.com/garyburd/redigo/redis"
)

var redisPool *redis.Pool

func init() {
    redisPool = &redis.Pool{
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				return nil, err
			}
			if _, err := c.Do("AUTH", "password"); err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func TestSetGet(t *testing.T)  {
    c := redisPool.Get()
    defer c.Close()

    _, err := c.Do("SET", "Test1", "Rodis Test1")
    if err != nil {
        t.Errorf("SET error: %v", err)
    }

    get, err := redis.String(c.Do("GET", "Test1"))
    if err != nil {
        t.Errorf("GET error: %v", err)
    }

    if get != "Rodis Test1" {
        t.Errorf("GET result '%v' is not equal to SET", get)
    }

    _, err = c.Do("SET", "Test1", "Rodis Test2")
    if err != nil {
        t.Errorf("SET again error: %v", err)
    }

    get, err = redis.String(c.Do("GET", "Test1"))
    if err != nil {
        t.Errorf("GET error: %v", err)
    }

    if get != "Rodis Test2" {
        t.Errorf("GET result '%v' is not equal to SET again", get)
    }
}
