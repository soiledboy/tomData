from github import Github
g = Github("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")

repo = g.get_user().get_repo('tim_dash')
all_files = []
contents = repo.get_contents("")
while contents:
    file_content = contents.pop(0)
    if file_content.type == "dir":
        contents.extend(repo.get_contents(file_content.path))
    else:
        file = file_content
        all_files.append(str(file).replace('ContentFile(path="','').replace('")',''))

with open('/home/tier1marketspace/youtuberReport/scripts/data/winners_final.csv', 'r') as file:
    content = file.read()

# Upload to github
git_prefix = 'data/'
git_file = git_prefix + 'winners_final.csv'
if git_file in all_files:
    contents = repo.get_contents(git_file)
    repo.update_file(contents.path, "committing files", content, contents.sha, branch="main")
    print(git_file + ' UPDATED')
else:
    repo.create_file(git_file, "committing files", content, branch="main")
    print(git_file + ' CREATED')