package controller

import (
	"cache-manager/config"
	"cache-manager/helper"
	"cache-manager/model"
	"context"
	"encoding/base64"
	"time"

	"github.com/gin-gonic/gin"
)

/*
Receive deleteCache request
*/
func DeleteCache(c *gin.Context) {
	helper.SecurePath(c)
	go deleteCacheProccess(c)
	c.JSON(200, gin.H{"status": 200,
		"message": "Delete cache worker is processing",
	})
}

/*
Receive addCache request
*/
func AddCache(c *gin.Context) {
	helper.SecurePath(c)
	var data model.CacheModel
	if err := c.ShouldBindJSON(&data); err != nil {
		c.JSON(400, gin.H{"status": 400, "message": "Invalid data" + err.Error()})
		return
	}
	go addCacheProccess(data)
	c.JSON(200, gin.H{"status": 200,
		"message": "Add cache worker is processing",
	})
}

/*
Delete cache based on item id search
*/
func deleteCacheProccess(c *gin.Context) {
	isCli := c.Param("isCli")
	if isCli != "1" {
		helper.SecurePath(c)
	}
	tag := c.Param("tag")
	var (
		url        string
		cacheCount uint
	)
	iter := config.SESSION.Query("select url from cache where tag='" + tag + "'").Iter()
	for iter.Scan(&url) {
		_, err := base64.StdEncoding.DecodeString(url)
		if err != nil {
			helper.Warning("cache delete failed, base64 decoding: "+err.Error(), nil)
		}
		helper.RemoveCachedItem(url)
		err = config.SESSION.Query("delete from cache where url = ?", url).Exec()
		if err != nil {
			helper.Warning("cache delete failed: "+err.Error(), nil)
		}
		// fmt.Println("Cache: ", string(decoded))
		url = ""
		cacheCount++
	}
	if err := iter.Close(); err != nil {
		helper.Warning("cache delete: "+err.Error(), nil)
		// panic(err.Error())
	}
}
func addCacheProccess(data model.CacheModel) {
	data.Url = base64.StdEncoding.EncodeToString([]byte((data.Url)))
	newData, err := base64.StdEncoding.DecodeString(data.Data)
	if err != nil {
		helper.Warning("cache saving failed, unable to decode data: "+err.Error(), nil)
		// panic(err.Error())
	}
	compressedIndexData := helper.CompressJsonIndexing(string(newData))
	err = config.SESSION.Query("insert into cache (url,data,tag,created_at,updated_at) values (?,?,?,toTimestamp(now()),toTimestamp(now()))", data.Url, compressedIndexData, data.Tag).Exec()
	if err != nil {
		helper.Warning("cache saving failed: Url:"+data.Url+","+err.Error(), nil)
		// panic(err.Error())
	}
	var minutes time.Duration = time.Duration(data.Duration)
	var ctx = context.Background()
	config.Redis.Set(ctx, helper.CachePrefix+data.Url, data.Data, minutes*time.Minute)
}

func ServiceStatusCheck(c *gin.Context) {
	c.JSON(400, gin.H{"status": 200, "message": "CA-MIS API service is running"})
}
