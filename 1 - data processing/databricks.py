storageAccountName = 'capstonestorageyn'
storageAccountAccessKey = '2Z3nXZ5UxaHw+13gJ2R5itqUCe36ygsW5L6AyO/h9Fa4Te+4h33GTQYNM1bkdPGAn1aBQ1wdOFNmaIpGyF2gzg=='
blobContainerName = 'historical'


if not any(mount.mountPoint == '/mnt/FileStore/MountFolder/' for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
    source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
    mount_point = "/mnt/FileStore/MountFolder/",
    extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
  )
  except Exception as e:
    print("already mounted. Try to unmount first")

display(dbutils.fs.ls("dbfs:/mnt/FileStore/MountFolder"))