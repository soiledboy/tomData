from winnersDataQuery import winData
#from winnersDataQueryDiff import winDataDiff
from winnersMetaData import winMetaData
from winnersMetaDataDiff import winMetaDataDiff
from winnersMerge import winMerge
from winnersMergeDiff import winMergeDiff
from winnersNewActivityMerge import winNewActivity
from mergeTodayAlltime import concatAllTimeToday
from googleCloud import collectorCloud

print("Starting on Winners Data")
winData()
print("finished")
#print("Starting on WinnersDiff Data")
#winDataDiff()
#print("finished")
print("Starting on Winners Meta Data")
winMetaData()
print("finished")
print("Starting on Winners MetaDiff Data")
winMetaDataDiff()
print("finished")
print("Starting on Winners Merge ")
winMerge()
print("finished")
print("Starting on WinnersMergeDiff ")
winMergeDiff()
print("finished")
print("Starting on Winners New Activity ")
winNewActivity()
print("finished")
print("Starting on final merge!!")
concatAllTimeToday()
print("finished")
print("Sending to Cloud")
collectorCloud()
print("Finished")