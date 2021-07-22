import json
import requests
import time
import uuid
from tqdm import tqdm
import jsonlines

def get_repo_file_tree(repo_name, main_branch):
    api_url = f"https://api.github.com/repos/{repo_name}/git/trees/{main_branch}?recursive=1"
    response = requests.get(api_url)


def write_jsonl_file(target_dir, dataset):
    target_path = f'{target_dir}/{uuid.uuid4()}.jsonl'

    with jsonlines.Writer(open(target_path, 'w')) as writer:
        writer.write_all(dataset)


def main(target_dir, source, allow_list):
    for i, repo in tqdm(enumerate(source)):
        try:
            api_response = get_repo_file_tree(repo['name'], repo['lastCommitSHA'])
        except:
            time.sleep(10)
            continue

        if api_response.status_code == 403:
            write_jsonl_file(target_dir, dataset)
            dataset = []
            time.sleep(606) # 1h
            print('last checkpoint', i)
            try:
                api_response = get_repo_file_tree(repo['name'], repo['lastCommitSHA'])
            except:
                continue

        if api_response.status_code == 404:
            continue

        repo_files = []

        try:
            repo_tree = api_response.json()['tree']
        except:
            continue

        for file in repo_tree:
            file_url = f'https://raw.githubusercontent.com/{repo["name"]}/{repo["lastCommitSHA"]}/{file["path"]}'
            ext = file_url.split('.')[-1]

            if ext in allow_list:
                repo_files.append({
                    'url': file_url,
                    'lang': allow_list[ext],
                    'size': file['size'] if 'size' in file else None
                })

        repo_points = {
            'files': repo_files,
            'num_files': len(repo_files),
            'license': repo['license'],
            'last_commit_date': repo['lastCommit'],
            'total_issues': repo['totalIssues'],
            'archived': repo['isArchived'],
            'stars': repo['stargazers'],
            'forks': repo['forks'],
        }

        dataset.append(repo_points)