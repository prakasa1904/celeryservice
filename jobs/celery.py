from __future__ import absolute_import

import os

from celery import Celery
from celery import group, chain, chord, subtask
import requests, urllib2
from bs4 import BeautifulSoup
from urlparse import urlparse

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'celeryservice.settings')

from django.conf import settings  # noqa

import jobs.sharedcelery

#app = Celery('jobs', broker='amqp://', backend='amqp')
app = Celery('jobs')

# Using a string here means the worker will not have to
# pickle the object when using Windows.
app.config_from_object('django.conf:settings')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)


@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))
    return self.request


@app.task(bind=True)
def img_total_size_before(self, url):
    # copyright by hadi
    r = requests.get('http://' + url)
    html = r.text
    parse = BeautifulSoup(html, 'html.parser')
    if r.status_code == 200:
        totalImg = parse.find_all('img')
        print 'total image in site = ', len(totalImg)
        for imgSrc in totalImg:
            totalSize = 0
            cekUrl = urlparse(imgSrc['src'])

            if cekUrl.scheme == '' and cekUrl.netloc != '' and  not imgSrc['src'].startswith('//'):
                imageSize = 'http://'+imgSrc['src']
            elif cekUrl.scheme == '' and cekUrl.netloc == '' and not imgSrc["src"].startswith("images/"):
                imageSize = 'http://'+url+imgSrc['src']
            elif cekUrl.netloc == '' and cekUrl.scheme == '' and imgSrc["src"].startswith("images/"):
                imageSize = 'http://'+url+'/'+imgSrc['src']
            elif imgSrc['src'].startswith('//'):
                imageSize = 'http:'+imgSrc['src']
            else:
                imageSize = imgSrc['src']

            try:
                fileSize = urllib2.urlopen(imageSize, timeout=2)
                totalSize += int(fileSize.headers.get("content-length"))
            except Exception as e:
                print('cannot open link '+str(e))

        print("Total Size  = ", totalSize)
        print('Request: {0!r}'.format(self.request))
        return totalSize

    else:
        print('access blocked')
        return None


@app.task(bind=True)
def img_total_size_after(self, url):
    r = requests.get('http://' + url)
    r.raise_for_status()

    html = r.text
    parse = BeautifulSoup(html, 'html.parser')

    totalImg = parse.find_all('img')
    print('total image in site = ', len(totalImg))
    #async_task = chord(jobs.sharedcelery.check_url.s(imgSrc['src'], url) | jobs.sharedcelery.pull_content_length.s()
    #      for imgSrc in totalImg)( jobs.sharedcelery.sum_content_length.s() )
    async_task = chord(chain(jobs.sharedcelery.check_url.subtask((imgSrc['src'], url,)),
                jobs.sharedcelery.pull_content_length.subtask(()))
          for imgSrc in totalImg)( jobs.sharedcelery.sum_content_length.subtask(()) )
    print('async_task Result: {0!r}'.format(async_task))
