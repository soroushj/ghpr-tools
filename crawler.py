import argparse
import inspect
import json
import logging
import os
from pathlib import Path
import re
import requests
import signal
import sys
import time

_base_url = 'https://api.github.com/'
_pulls_url_template = _base_url + 'repos/{owner}/{repo}/pulls?state=closed&sort=created&direction=asc&per_page={per_page}&page={page}'
_pull_url_template = _base_url + 'repos/{owner}/{repo}/pulls/{pull_number}'
_issue_url_template = _base_url + 'repos/{owner}/{repo}/issues/{issue_number}'

_repo_path_template = os.path.join('{dst_dir}', '{owner}', '{repo}')
_pulls_path_template = os.path.join('{dst_dir}', '{owner}', '{repo}', 'pulls-page-{page}.json')
_pull_path_template = os.path.join('{dst_dir}', '{owner}', '{repo}', 'pull-{pull_number}.json')
_issue_path_template = os.path.join('{dst_dir}', '{owner}', '{repo}', 'issue-{issue_number}.json')

_linked_issues_pattern_template = r'\b(?:close|closes|closed|fix|fixes|fixed|resolve|resolves|resolved)\s+(?:https://github\.com/{owner}/{repo}/issues/|{owner}/{repo}#|#)(\d+)\b'

def _make_linked_issues_regex(owner, repo):
    owner = owner.replace('.', r'\.')
    repo = repo.replace('.', r'\.')
    pattern = _linked_issues_pattern_template.format(owner=owner, repo=repo)
    return re.compile(pattern, flags=re.IGNORECASE)

def _extract_linked_issue_numbers(pull_body, linked_issues_regex):
    if pull_body is None:
        return []
    return [int(n) for n in linked_issues_regex.findall(pull_body)]

def _save_json(obj, path):
    with open(path, 'w') as f:
        json.dump(obj, f, indent=2, sort_keys=True)

def _ensure_dir_exists(path):
    Path(path).mkdir(parents=True, exist_ok=True)

class TooManyRequestFailures(Exception):
    pass

class Crawler(object):
    """Crawl GitHub repositories to find and save issues and pull requests that have
    fixed them.

    The crawler goes through the pages of closed pull requests, from oldest to
    newest. If a pull request is merged and links one or more issues in its
    description, the pull request and its linked issue(s) will be fetched and
    saved as JSON files. The list of linked issue numbers is added to the fetched
    pull request JSON object with the key "linked_issue_numbers". The JSON files
    will be saved in DEST_DIR/owner/repo. The directories will be created if they
    do not already exist. The naming pattern for files is issue-N.json for issues,
    pull-N.json for pull requests, and pulls-page-N.json for pages of pull
    requests. Any existing file will be overwritten. The GitHub API limits
    unauthenticated clients to 60 requests per hour. The rate limit is 5,000
    requests per hour for authenticated clients. For this reason, you should
    provide a GitHub OAuth token if you want to crawl a large repository. You can
    create a personal access token at https://github.com/settings/tokens.

    Attributes:
        dst_dir (str): Directory for saving JSON files.
        per_page (int): Pull requests per page, between 1 and 100.
        save_pull_pages (bool): Save the pages of pull requests.
        max_request_tries (int): Number of times to try a request before
            terminating.
        request_retry_wait_secs (int): Seconds to wait before retrying a failed request.
    """

    def __init__(self,
                 token=None,
                 dst_dir='data-json',
                 per_page=100,
                 save_pull_pages=False,
                 max_request_tries=100,
                 request_retry_wait_secs=10):
        """Initializes Crawler.

        The GitHub API limits unauthenticated clients to 60 requests per hour. The
        rate limit is 5,000 requests per hour for authenticated clients. For this
        reason, you should provide a GitHub OAuth token if you want to crawl a large
        repository. You can create a personal access token at
        https://github.com/settings/tokens.

        Args:
            token (str): Your GitHub OAuth token. If None, the crawler will be
                unauthenticated.
            dst_dir (str): Directory for saving JSON files.
            per_page (int): Pull requests per page, between 1 and 100.
            save_pull_pages (bool): Save the pages of pull requests.
            max_request_tries (int): Number of times to try a request before
                terminating.
            request_retry_wait_secs (int): Seconds to wait before retrying a failed request.
        """
        self.dst_dir = dst_dir
        self.per_page = per_page
        self.save_pull_pages = save_pull_pages
        self.max_request_tries = max_request_tries
        self.request_retry_wait_secs = request_retry_wait_secs
        self._headers = {
            'Accept': 'application/vnd.github.v3+json',
        }
        if token is not None:
            self._headers['Authorization'] = 'token ' + token
        self._interrupted = False
        def sigint_handler(signal, frame):
            if self._interrupted:
                print('\nForced exit')
                sys.exit(2)
            self._interrupted = True
            print('\nInterrupted, finishing current page\nPress interrupt key again to force exit')
        signal.signal(signal.SIGINT, sigint_handler)

    def crawl(self, owner, repo, start_page=1):
        """Crawls a GitHub repository and saves issues and pull requests that have fixed
        them.

        The crawler goes through the pages of closed pull requests, from oldest to
        newest. If a pull request is merged and links one or more issues in its
        description, the pull request and its linked issue(s) will be fetched and
        saved as JSON files. The list of linked issue numbers is added to the fetched
        pull request JSON object with the key "linked_issue_numbers". The JSON files
        will be saved in DEST_DIR/owner/repo. The directories will be created if they
        do not already exist. The naming pattern for files is issue-N.json for issues,
        pull-N.json for pull requests, and pulls-page-N.json for pages of pull
        requests. Any existing file will be overwritten.

        Args:
            owner (str): The username of the repository owner, e.g., "octocat" for the
                https://github.com/octocat/Hello-World repository.
            repo (str): The name of the repository, e.g., "Hello-World" for the
                https://github.com/octocat/Hello-World repository.
            start_page (int): Page to start crawling from.

        Raises:
            TooManyRequestFailures: A request failed max_request_tries times.
        """
        logging.info('Crawl: starting {} {}/{}'.format(start_page, owner, repo))
        print('Starting from page {} ({}/{})'.format(start_page, owner, repo))
        _ensure_dir_exists(_repo_path_template.format(dst_dir=self.dst_dir, owner=owner, repo=repo))
        linked_issues_regex = _make_linked_issues_regex(owner, repo)
        page = start_page
        num_issues = 0
        num_pulls = 0
        self._interrupted = False
        while not self._interrupted:
            pulls = self._get(_pulls_url_template.format(per_page=self.per_page, owner=owner, repo=repo, page=page))
            pulls_issue_numbers = {} # {pull1_number: [issue1_number, issue2_number]}
            for p in pulls:
                linked_issue_numbers = _extract_linked_issue_numbers(p.get('body'), linked_issues_regex)
                if linked_issue_numbers and p['merged_at']:
                    pulls_issue_numbers[p['number']] = linked_issue_numbers
                if self.save_pull_pages:
                    p['linked_issue_numbers'] = linked_issue_numbers
            if self.save_pull_pages:
                _save_json(pulls, _pulls_path_template.format(dst_dir=self.dst_dir, owner=owner, repo=repo, page=page))
            for pull_number, issue_numbers in pulls_issue_numbers.items():
                pull = self._get(_pull_url_template.format(owner=owner, repo=repo, pull_number=pull_number))
                pull['linked_issue_numbers'] = issue_numbers
                _save_json(pull, _pull_path_template.format(dst_dir=self.dst_dir, owner=owner, repo=repo, pull_number=pull_number))
                num_pulls += 1
                for issue_number in issue_numbers:
                    issue = self._get(_issue_url_template.format(owner=owner, repo=repo, issue_number=issue_number))
                    _save_json(issue, _issue_path_template.format(dst_dir=self.dst_dir, owner=owner, repo=repo, issue_number=issue_number))
                    num_issues += 1
            logging.info('Crawl: finished {} {}/{}'.format(page, owner, repo))
            print('Page {} finished ({}/{})'.format(page, owner, repo))
            if len(pulls) < self.per_page:
                logging.info('Crawl: finished all, {} issues {} pulls {}/{}'.format(num_issues, num_pulls, owner, repo))
                print('All pages finished, saved {} issues and {} pull requests ({}/{})'.format(num_issues, num_pulls, owner, repo))
                return
            page += 1

    def _get(self, url):
        tries = 0
        while True:
            r = self._try_to_get(url)
            if r is not None:
                return r
            tries += 1
            if tries >= self.max_request_tries:
                print('Request failed {} times, aborting'.format(tries))
                raise TooManyRequestFailures('{} request failures for {}'.format(tries, url))
            print('Request failed {} times, retrying in {} seconds'.format(tries, self.request_retry_wait_secs))
            time.sleep(self.request_retry_wait_secs)

    def _try_to_get(self, url):
        try:
            r = requests.get(url, headers=self._headers)
            if not r.ok:
                logging.error('Get: not ok: {} {} {} {}'.format(url, r.status_code, r.headers, r.text))
                if 'X-Ratelimit-Remaining' in r.headers and int(r.headers['X-Ratelimit-Remaining']) < 1 and 'X-Ratelimit-Reset' in r.headers:
                    ratelimit_wait_secs = int(r.headers['X-Ratelimit-Reset']) - int(time.time()) + 1
                    logging.info('Get: waiting {} secs for rate limit reset'.format(ratelimit_wait_secs))
                    print('Rate limit reached, waiting {} secs for reset'.format(ratelimit_wait_secs))
                    time.sleep(ratelimit_wait_secs)
                    return self._try_to_get(url)
                return None
            rj = r.json()
        except Exception as e:
            logging.error('Get: exception: {} {}'.format(url, e))
            return None
        if isinstance(rj, dict) and 'message' in rj:
            logging.error('Get: error: {} {}'.format(url, rj))
            return None
        return rj

def main():
    init_params = inspect.signature(Crawler.__init__).parameters
    crawl_params = inspect.signature(Crawler.crawl).parameters
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Crawl GitHub repositories to find and save issues and pull requests that have fixed them. '
                    'The crawler goes through the pages of closed pull requests, from oldest to newest. '
                    'If a pull request is merged and links one or more issues in its description, '
                    'the pull request and its linked issue(s) will be fetched and saved as JSON files. '
                    'The list of linked issue numbers is added to the fetched pull request JSON object with the key "linked_issue_numbers". '
                    'The JSON files will be saved in DEST_DIR/owner/repo. '
                    'The directories will be created if they do not already exist. '
                    'The naming pattern for files is issue-N.json for issues, pull-N.json for pull requests, '
                    'and pulls-page-N.json for pages of pull requests. '
                    'Any existing file will be overwritten. '
                    'The GitHub API limits unauthenticated clients to 60 requests per hour. '
                    'The rate limit is 5,000 requests per hour for authenticated clients. '
                    'For this reason, you should provide a GitHub OAuth token if you want to crawl a large repository. '
                    'You can create a personal access token at https://github.com/settings/tokens.')
    parser.add_argument('-t', '--token', type=str, default=init_params['token'].default,
        help='your GitHub OAuth token, can also be provided via a GITHUB_OAUTH_TOKEN environment variable')
    parser.add_argument('-d', '--dst-dir', type=str, default=init_params['dst_dir'].default,
        help='directory for saving JSON files')
    parser.add_argument('-s', '--start-page', type=int, default=crawl_params['start_page'].default,
        help='page to start crawling from')
    parser.add_argument('-p', '--per-page', type=int, default=init_params['per_page'].default,
        help='pull requests per page, between 1 and 100')
    parser.add_argument('-a', '--save-pull-pages', action='store_true',
        help='save the pages of pull requests')
    parser.add_argument('-m', '--max-request-tries', type=int, default=init_params['max_request_tries'].default,
        help='number of times to try a request before terminating')
    parser.add_argument('-r', '--request-retry-wait-secs', type=int, default=init_params['request_retry_wait_secs'].default,
        help='seconds to wait before retrying a failed request')
    parser.add_argument('-l', '--log-file', type=str, default=None,
        help='file to write logs to')
    parser.add_argument('repos', metavar='repo', type=str, nargs='+',
        help='full repository name, e.g., "octocat/Hello-World" for the https://github.com/octocat/Hello-World repository')
    args = parser.parse_args()

    if args.token is None:
        args.token = os.environ.get('GITHUB_OAUTH_TOKEN')
    if args.token == '':
        args.token = None

    if args.log_file is not None:
        logging.basicConfig(filename=args.log_file, filemode='w', level=logging.DEBUG)

    crawler = Crawler(token=args.token,
                      dst_dir=args.dst_dir,
                      per_page=args.per_page,
                      save_pull_pages=args.save_pull_pages,
                      max_request_tries=args.max_request_tries,
                      request_retry_wait_secs=args.request_retry_wait_secs)
    for r in args.repos:
        n = r.find('/')
        owner = r[:n]
        repo = r[n+1:]
        try:
            crawler.crawl(owner, repo, start_page=args.start_page)
        except Exception as e:
            logging.error('Main: exception: {}/{} {}'.format(owner, repo, e))
            print('Terminated with error: {} ({}/{})'.format(e, owner, repo))

if __name__ == '__main__':
    main()
