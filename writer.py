import argparse
from bs4 import BeautifulSoup
import calendar
import csv
import json
import markdown
import os
import sys
import time
from tqdm import tqdm

_owner_path_template = os.path.join('{src_dir}', '{owner}')
_repo_path_template = os.path.join('{src_dir}', '{owner}', '{repo}')
_pull_path_template = os.path.join('{src_dir}', '{owner}', '{repo}', 'pull-{pull_number}.json')
_issue_path_template = os.path.join('{src_dir}', '{owner}', '{repo}', 'issue-{issue_number}.json')

_dataset_header = [
    'issue_number',
    'issue_title',
    'issue_body_md',
    'issue_body_plain',
    'issue_created_at',
    'issue_author_id',
    'issue_author_association',
    'issue_label_ids',
    'pull_number',
    'pull_created_at',
    'pull_merged_at',
    'pull_comments',
    'pull_review_comments',
    'pull_commits',
    'pull_additions',
    'pull_deletions',
    'pull_changed_files',
]

_author_association_value = {
    'COLLABORATOR': 0,
    'CONTRIBUTOR': 1,
    'FIRST_TIMER': 2,
    'FIRST_TIME_CONTRIBUTOR': 3,
    'MANNEQUIN': 4,
    'MEMBER': 5,
    'NONE': 6,
    'OWNER': 7,
}

def write_dataset(src_dir, dst_file):
    """Reads JSON files downloaded by the Crawler and writes a CSV file from their
    data.

    The CSV file will have the following columns:
    - issue_number: integer
    - issue_title: text
    - issue_body_md: text, in Markdown format, can be empty
    - issue_body_plain: text, in plain text, can be empty
    - issue_created_at: integer, in Unix time
    - issue_author_id: integer
    - issue_author_association: integer enum (see values below)
    - issue_label_ids: comma-seperated integers, can be empty
    - pull_number: integer
    - pull_created_at: integer, in Unix time
    - pull_merged_at: integer, in Unix time
    - pull_comments: integer
    - pull_review_comments: integer
    - pull_commits: integer
    - pull_additions: integer
    - pull_deletions: integer
    - pull_changed_files: integer
    The value of issue_body_plain is converted from issue_body_md. The conversion is
    not always perfect. In some cases, issue_body_plain still contains some Markdown
    tags.
    The value of issue_author_association can be one of the following:
    - 0: Collaborator
    - 1: Contributor
    - 2: First-timer
    - 3: First-time contributor
    - 4: Mannequin
    - 5: Member
    - 6: None
    - 7: Owner
    Rows are sorted by owner username, repository name, and then pull request
    number.
    The source directory must contain owner/repo/issue-N.json and
    owner/repo/pull-N.json files. The destination directory of Crawler should
    normally be used as the source directory of Writer. The destination file will be
    overwritten if it already exists.

    Args:
        src_dir (str): Source directory.
        dst_file (str): Destination CSV file.
    """
    num_rows = 0
    with open(dst_file, 'w', newline='') as dataset_file:
        dataset = csv.writer(dataset_file)
        dataset.writerow(_dataset_header)
        owner_repo_pairs = _sorted_owner_repo_pairs(src_dir)
        num_repos = len(owner_repo_pairs)
        for i, (owner, repo) in enumerate(owner_repo_pairs):
            print('{}/{} ({}/{})'.format(owner, repo, i + 1, num_repos))
            for pull_number in tqdm(_sorted_pull_numbers(src_dir, owner, repo)):
                pull = _read_json(_pull_path_template.format(src_dir=src_dir, owner=owner, repo=repo, pull_number=pull_number))
                for issue_number in pull['linked_issue_numbers']:
                    issue = _read_json(_issue_path_template.format(src_dir=src_dir, owner=owner, repo=repo, issue_number=issue_number))
                    dataset.writerow(_dataset_row(issue, pull))
                    num_rows += 1
    print('Wrote {} rows'.format(num_rows))

def _sorted_owner_repo_pairs(src_dir):
    pairs = [] # [(owner1,repo1), (owner2,repo2)]
    owners = os.listdir(src_dir)
    owners.sort()
    for owner in owners:
        repos = os.listdir(_owner_path_template.format(src_dir=src_dir, owner=owner))
        repos.sort()
        for repo in repos:
            pairs.append((owner, repo))
    return pairs

def _sorted_pull_numbers(src_dir, owner, repo):
    filenames = os.listdir(_repo_path_template.format(src_dir=src_dir, owner=owner, repo=repo))
    pull_numbers = [int(f[5:-5]) for f in filenames if f.startswith('pull-')]
    pull_numbers.sort()
    return pull_numbers

def _read_json(path):
    with open(path, 'r') as f:
        return json.load(f)

def _dataset_row(issue, pull):
    if issue.get('body') is None:
        issue_body_md = ''
        issue_body_plain = ''
    else:
        issue_body_md = issue['body']
        issue_body_plain = _md_to_text(issue_body_md)
    issue_label_ids = ','.join(str(l['id']) for l in issue['labels'])
    return [
        issue['number'],
        issue['title'],
        issue_body_md,
        issue_body_plain,
        _iso_to_unix(issue['created_at']),
        issue['user']['id'],
        _author_association_value[issue['author_association']],
        issue_label_ids,
        pull['number'],
        _iso_to_unix(pull['created_at']),
        _iso_to_unix(pull['merged_at']),
        pull['comments'],
        pull['review_comments'],
        pull['commits'],
        pull['additions'],
        pull['deletions'],
        pull['changed_files'],
    ]

def _md_to_text(md):
    html = markdown.markdown(md)
    soup = BeautifulSoup(html, features='html.parser')
    return soup.get_text()

def _iso_to_unix(iso):
    utc_time = time.strptime(iso, '%Y-%m-%dT%H:%M:%SZ')
    return calendar.timegm(utc_time)

def main():
    parser = argparse.ArgumentParser(
        description='Read JSON files downloaded by the Crawler and write a CSV file from their data. '
                    'The source directory must contain owner/repo/issue-N.json and owner/repo/pull-N.json files. '
                    'The destination directory of Crawler should normally be used as the source directory of Writer. '
                    'The destination file will be overwritten if it already exists.')
    parser.add_argument('src_dir', type=str,
        help='source directory')
    parser.add_argument('dst_file', type=str,
        help='destination CSV file')
    args = parser.parse_args()
    write_dataset(args.src_dir, args.dst_file)

if __name__ == '__main__':
    main()
