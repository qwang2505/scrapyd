import os
from urlparse import urlparse, urlunparse

from zope.interface import implements

from .interfaces import IEnvironment

class Environment(object):

    implements(IEnvironment)

    def __init__(self, config, initenv=os.environ):
        self.dbs_dir = config.get('dbs_dir', 'dbs')
        self.logs_dir = config.get('logs_dir', 'logs')
        self.items_dir = config.get('items_dir', 'items')
        self.jobs_to_keep = config.getint('jobs_to_keep', 5)
        if config.cp.has_section('settings'):
            self.settings = dict(config.cp.items('settings'))
        else:
            # TODO no defualt settings, what can be in settings?
            self.settings = {}
        # use system environment to init envs.
        self.initenv = initenv

    def get_environment(self, message, slot):
        """
        What's this for? I guess this is used before carwl spider.

        This been called in launcher._spawn_process, so my guess is right.
        """
        # get project
        project = message['_project']
        # get initial env
        env = self.initenv.copy()
        # slot is just a index, 0, 1, etc.
        env['SCRAPY_SLOT'] = str(slot)
        # project name
        env['SCRAPY_PROJECT'] = project
        # spider name
        env['SCRAPY_SPIDER'] = message['_spider']
        # TODO job name, but what is job?
        env['SCRAPY_JOB'] = message['_job']
        # if have project specific settings set it.
        if project in self.settings:
            env['SCRAPY_SETTINGS_MODULE'] = self.settings[project]
        # set log file
        if self.logs_dir:
            env['SCRAPY_LOG_FILE'] = self._get_file(message, self.logs_dir, 'log')
        # set item file
        if self.items_dir:
            env['SCRAPY_FEED_URI'] = self._get_feed_uri(message, 'jl')
        return env

    def _get_feed_uri(self, message, ext):
        url = urlparse(self.items_dir)
        if url.scheme.lower() in ['', 'file']:
            return 'file://' + self._get_file(message, url.path, ext)
        return urlunparse((url.scheme,
                           url.netloc,
                           '/'.join([url.path,
                                     message['_project'],
                                     message['_spider'],
                                     '%s.%s' % (message['_job'], ext)]),
                           url.params,
                           url.query,
                           url.fragment))

    def _get_file(self, message, dir, ext):
        # use base dir, project name, spider name to construct log dir.
        logsdir = os.path.join(dir, message['_project'], \
            message['_spider'])
        if not os.path.exists(logsdir):
            os.makedirs(logsdir)
        # TODO need to understand what is job?
        to_delete = sorted((os.path.join(logsdir, x) for x in \
            os.listdir(logsdir)), key=os.path.getmtime)[:-self.jobs_to_keep]
        for x in to_delete:
            os.remove(x)
        # just guess, job may be a random timestamp to indicate time.
        return os.path.join(logsdir, "%s.%s" % (message['_job'], ext))
