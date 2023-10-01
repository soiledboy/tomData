import dropbox
access_token = 'Y11Q32XabqQAAAAAAAAAAZcnKEpTcXefA57NB6lnkbX_CBDfUyCLgRUZ-EX-P2yt'

print("starting on file 1")
file_from = '/home/tier1marketspace/youtuberReport/scripts/data/winners_final1.csv'
file_to = '/tier1marketspace/winners_final1.csv'
def update_file1(file_from, file_to):
    dbx = dropbox.Dropbox(access_token)
    f = open(file_from, 'rb')
    dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)
update_file1(file_from,file_to)
print("finished")
print("starting on file2")
file_from = '/home/tier1marketspace/youtuberReport/scripts/data/winners_final2.csv'
file_to = '/tier1marketspace/winners_final2.csv'
def update_file2(file_from, file_to):
    dbx = dropbox.Dropbox(access_token)
    f = open(file_from, 'rb')
    dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)
update_file2(file_from,file_to)
print("finished")