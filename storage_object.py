import xmlrpc


# class StorageObject:
#     """
#     Represents the storage on the naming server. Responsible for storing
#     information about chunks' location on storage servers and nothing else.
#     """
#
#     def __init__(self, server_id, address):
#         self.id = server_id
#         self.address = address
#         self.proxy = self.init_proxy()
#         self.files = dict()
#
#     def init_proxy(self):
#         address = self.address.split(":")
#         return xmlrpc.client.ServerProxy('http://' + address[0] + ':' + address[1])
#
#     def add_chunk(self, path, chunk_info):
#         file = self.files.get(path)
#         if file is None:
#             self.files[path] = [chunk_info]
#         else:
#             file.append(chunk_info)
#
#     def delete_chunks(self, path):
#         file = self.files.get(path)
#         if file is None:
#             raise FileNotFoundError
#         else:
#             # deleting chunks from storage server
#             for chunk_info in file:
#                 self.proxy.delete(chunk_info[1])
#             # deleting entry from dictionary
#             del (self.files[path])
#
#     def get_chunks(self, path):
#         file = self.files.get(path)
#         if file is None:
#             return []
#         else:
#             return file
#
#     def get_all_chunks(self):
#
#         pass
