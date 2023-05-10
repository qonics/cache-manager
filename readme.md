# Cache manager

Cache Manager is a powerful utility designed to help improve the performance of REST APIs by implementing caching mechanisms. It provides an efficient way to store and retrieve frequently accessed data, reducing the load on the server and improving response times.

## How it works
Cache Manager acts as an intermediary between the REST API and the data source. When a request is made to the API, the Cache Manager first checks if the requested data is already stored in the cache. If the data is found, it is retrieved from the cache and returned to the client without the need to access the data source.

If the data is not found in the cache, the Cache Manager retrieves it from the data source, stores it in the cache for future use, and returns it to the client. This caching mechanism significantly reduces the number of requests sent to the data source, resulting in improved performance and decreased response times.

**the available access option is only REST API, we will keep improving it and you can create a pull request**

### Supported storage engine/Database
1. Redis

## To do
- Authentication
- Delete cached item by using tags
- Delete cached item by using a snipped of content data like id
- Supporting of access it by using sockets
- Terminal access (View summary, View version, purge cache, view cache, update,...)
- Support multiple storage engine/database