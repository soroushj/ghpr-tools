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
    'repo_id',
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

def write_dataset(src_dir, dst_file, limit_rows=0):
    """Reads JSON files downloaded by the Crawler and writes a CSV file from their
    data.

    The CSV file will have the following columns:
    - repo_id: Integer
    - issue_number: Integer
    - issue_title: Text
    - issue_body_md: Text, in Markdown format, can be empty
    - issue_body_plain: Text, in plain text, can be empty
    - issue_created_at: Integer, in Unix time
    - issue_author_id: Integer
    - issue_author_association: Integer enum (see values below)
    - issue_label_ids: Comma-separated integers, can be empty
    - pull_number: Integer
    - pull_created_at: Integer, in Unix time
    - pull_merged_at: Integer, in Unix time
    - pull_comments: Integer
    - pull_review_comments: Integer
    - pull_commits: Integer
    - pull_additions: Integer
    - pull_deletions: Integer
    - pull_changed_files: Integer
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
    Rows are sorted by repository owner username, repository name, pull request
    number, and then issue number.
    The source directory must contain owner/repo/issue-N.json and
    owner/repo/pull-N.json files. The destination directory of Crawler should
    normally be used as the source directory of Writer. The destination file will be
    overwritten if it already exists.

    Args:
        src_dir (str): Source directory.
        dst_file (str): Destination CSV file.
        limit_rows (int): Maximum number of rows to write.
    """
    repo_full_names = []
    repo_num_rows = []
    total_num_rows = 0
    def print_results():
        for r, n in zip(repo_full_names, repo_num_rows):
            print('{}: {:,}'.format(r, n))
        print('Total: {:,}'.format(total_num_rows))
    with open(dst_file, 'w', newline='') as dataset_file:
        dataset = csv.writer(dataset_file)
        dataset.writerow(_dataset_header)
        owner_repo_pairs = _sorted_owner_repo_pairs(src_dir)
        num_repos = len(owner_repo_pairs)
        for i, (owner, repo) in enumerate(owner_repo_pairs):
            repo_full_name = '{}/{}'.format(owner, repo)
            repo_full_names.append(repo_full_name)
            repo_num_rows.append(0)
            print('{} ({:,}/{:,})'.format(repo_full_name, i + 1, num_repos))
            for pull_number in tqdm(_sorted_pull_numbers(src_dir, owner, repo)):
                pull = _read_json(_pull_path_template.format(src_dir=src_dir, owner=owner, repo=repo, pull_number=pull_number))
                pull['linked_issue_numbers'].sort()
                for issue_number in pull['linked_issue_numbers']:
                    issue = _read_json(_issue_path_template.format(src_dir=src_dir, owner=owner, repo=repo, issue_number=issue_number))
                    dataset.writerow(_dataset_row(issue, pull))
                    repo_num_rows[i] += 1
                    total_num_rows += 1
                    if total_num_rows == limit_rows:
                        print('Limit of {:,} rows reached'.format(limit_rows))
                        print_results()
                        return
    print('Finished')
    print_results()

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
        pull['base']['repo']['id'],
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
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Read JSON files downloaded by the Crawler and write a CSV file from their data. '
                    'The source directory must contain owner/repo/issue-N.json and owner/repo/pull-N.json files. '
                    'The destination directory of Crawler should normally be used as the source directory of Writer. '
                    'The destination file will be overwritten if it already exists.')
    parser.add_argument('-l', '--limit-rows', type=int, default=0,
        help='limit number of rows to write, ignored if non-positive')
    parser.add_argument('src_dir', type=str,
        help='source directory')
    parser.add_argument('dst_file', type=str,
        help='destination CSV file')
    args = parser.parse_args()
    write_dataset(args.src_dir, args.dst_file, limit_rows=args.limit_rows)

if __name__ == '__main__':
    main()
