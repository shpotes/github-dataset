import argparse
import json
import os
import requests
import time
import uuid
from tqdm import tqdm
import jsonlines

def get_repo_file_tree(repo_name, main_branch):
    api_url = f"https://api.github.com/repos/{repo_name}/git/trees/{main_branch}?recursive=1"
    github_key = os.environ['GITHUB_KEY']
    api_auth = requests.auth.HTTPBasicAuth('shpotes', github_key)
    response = requests.get(api_url, auth=api_auth)


def write_jsonl_file(target_dir, dataset):
    target_path = f'{target_dir}/{uuid.uuid4()}.jsonl'

    with jsonlines.Writer(open(target_path, 'w')) as writer:
        writer.write_all(dataset)


def main(target_dir, source, allow_list, min_dump_size=1_000):
    for i, repo in tqdm(enumerate(source)):
        try:
            api_response = get_repo_file_tree(repo['name'], repo['lastCommitSHA'])
        except:
            continue

        if api_response.status_code == 403:
            if dataset > min_dump_size:
                write_jsonl_file(target_dir, dataset)
                dataset = []

            time.sleep(1000)
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Github downloader')
    parser.add_argument(
        '--target_dir', type=str, help='dumps target folder'
    )
    parser.add_argument(
        '--source', type=str, help='source json file'
    )
    parser.add_argument(
        '--allow_list', type=str, help='allow list json file'
    )
    parser.add_argument(
        '--min_dump_size', type=int, help='minimum dump size', default=1_000
    )
    args = parser.parse_args()

    source = json.load(open(args.source, 'w'))
    allow_list = json.load(open(args.source, 'w'))

    main(args.target_dir, source, allow_list, args.min_dump_size)