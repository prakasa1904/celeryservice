from __future__ import absolute_import

from celery import shared_task
from urlparse import urlparse

import urllib2


@shared_task
def add(x, y):
    return x + y


@shared_task
def mul(x, y):
    return x * y


@shared_task
def xsum(numbers):
    return sum(numbers)


@shared_task
def check_url(src, url):
    print(src)
    cekUrl = urlparse(src)

    if cekUrl.scheme == '' and cekUrl.netloc != '' and  not src.startswith('//'):
        imageUrl = 'http://'+src
    elif cekUrl.scheme == '' and cekUrl.netloc == '' and not src.startswith("images/"):
        imageUrl = 'http://'+url+src
    elif cekUrl.netloc == '' and cekUrl.scheme == '' and src.startswith("images/"):
        imageUrl = 'http://'+url+'/'+src
    elif src.startswith('//'):
        imageUrl = 'http:'+src
    else:
        imageUrl = src
    return imageUrl


@shared_task(default_retry_delay=1 * 60, max_retries=3)
def pull_content_length(image_url):
    print(image_url)
    try:
        fileSize = urllib2.urlopen(image_url, timeout=2)
        return int(fileSize.headers.get("content-length"))
    except Exception, exc:
        raise pull_content_length.retry(exc=exc)
        #return 0


@shared_task
def sum_content_length(sources_len):
    print(sources_len)
    print(sum(sources_len))
    return sum(sources_len)
