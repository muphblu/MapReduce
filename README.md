# MapReduce

## Methods
- Files:
	- Read
	- Write
	- Delete
	- SizeQuery
- Directory
	- List
	- Create
	- Delete
- ObjectType(folder, file, neither)

## Structure:

### StorageServer
started on ports `8000 - (8000+n)` where `n` number of servers
- byte[] read(id) // read chunk from server
- write(chunk_id, content) //where list contains tuples(chunkId, content)
- size()
- status delete(list[]) // list of chunkIds
- size: ??

### NamingServer
started on `8000` port
- `listTuples read(path)` // returns ordered list of tuples where key is serverId and value is chunkId
- `listTuples write(path, countChunks)` // returns ordered list with tuples where key is serverId and value is chunksIds
- `status delete(path)` // Name server tells storages to delete particular chunks
- `size: ??`
- `list[] list(path)` // list of files/directories
- `status mkdir(path)`
- `status rmdir(path)`
- `type get_type(path)`

## Questions
1. Slava: Can we use plain structure for storage servers (we can use)
2. Slava: can we store file size on the namingServer?
3. Slava: Can we unite rmdir and delete?
