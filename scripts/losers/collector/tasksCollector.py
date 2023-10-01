from losersDataQuery import loseData
from losersDataQueryDiff import loseDataDiff
from losersMetaData import loseMetaData
from losersMetaDataDiff import loseMetaDataDiff
from losersMerge import loseMerge
from losersMergeDiff import loseMergeDiff
from losersNewActivityMerge import loseNewActivity
from mergeTodayAlltime import concatAllTimeToday
from googleCloud import metaCloud

print("Starting on losers Data")
loseData()
print("finished")
print("Starting on losersDiff Data")
loseDataDiff()
print("finished")
print("Starting on losers Meta Data")
loseMetaData()
print("finished")
print("Starting on losers MetaDiff Data")
loseMetaDataDiff()
print("finished")
print("Starting on losers Merge ")
loseMerge()
print("finished")
print("Starting on losersMergeDiff ")
loseMergeDiff()
print("finished")
print("Starting on losers New Activity ")
loseNewActivity()
print("finished")
print("Starting on final merge!!")
concatAllTimeToday()
print("finished")
print("Sending to Cloud")
metaCloud()
print("Finished")