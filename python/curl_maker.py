import pycurl
import certifi
import urllib
from io import StringIO, BytesIO
import os
import re

class curler:

    def header_function(self, header_line):
        # HTTP standard specifies that headers are encoded in iso-8859-1.
        # On Python 2, decoding step can be skipped.
        # On Python 3, decoding step is required.
        header_line = header_line.decode('iso-8859-1')

        # Header lines include the first status line (HTTP/1.x ...).
        # We are going to ignore all lines that don't have a colon in them.
        # This will botch headers that are split on multiple lines...
        if ':' not in header_line:
            return

        # Break the header line into header name and value.
        name, value = header_line.split(':', 1)

        # Remove whitespace that may be present.
        # Header lines include the trailing newline, and there may be whitespace
        # around the colon.
        name = name.strip()
        value = value.strip()

        # Header names are case insensitive.
        # Lowercase name here.
        name = name.lower()

        # Now we can actually record the header name and value.
        # Note: this only works when headers are not duplicated, see below.
        self.header[name] = value

    def __init__(self, url=None):
        self.content_writer = BytesIO()
        self.content = b''
        self.header_func = self.header_function
        self.curler = pycurl.Curl()
        self.curler.setopt(pycurl.HEADERFUNCTION, self.header_func)
        self.curler.setopt(pycurl.CAINFO, certifi.where())
        self.curler.setopt(pycurl.COOKIEFILE, '')
        self.url = url
        if url is not None:
            self.curler.setopt(pycurl.URL, url)
    
    def login(self, url=None, username=None, password=None, digest=False, header_mod = []):
        self.header= {}
        if digest:
            self.curler.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_DIGEST)
        if len(header_mod) > 0:
            self.curler.setopt(pycurl.HTTPHEADER, header_mod)
        if url is None:
            url = self.url
        self.curler.setopt(pycurl.URL, url)
        up = '{}:{}'.format(username, password)
        self.curler.setopt(pycurl.USERPWD, up)
        
        if self.content_writer.closed:
            self.content_writer = BytesIO()
        self.curler.setopt(pycurl.WRITEDATA, self.content_writer)
        
        try:
            self.curler.perform()
            self.content = self.content_writer.getvalue()
        except:
            print(self.header)
        finally:
            self.content_writer.close()
    
    def logout(self, url=None):
        self.header= {}
        if url is None:
            url = self.url
        self.curler.setopt(pycurl.URL, url)
        if self.content_writer.closed:
            self.content_writer = BytesIO()
        self.curler.setopt(pycurl.WRITEDATA, self.content_writer)
        try:
            self.curler.perform()
            self.content = self.content_writer.getvalue()
        except:
            print(self.header)
        finally:
            self.content_writer.close()
            
            
    def get_request(self,  url, params={}):
        self.header= {}
        self.curler.setopt(pycurl.POST, 0)
        if url is None:
            url = self.url
        if len(params) > 0:
            url = url + '?' + urllib.urlencode(params)
        if self.content_writer.closed:
            self.content_writer = BytesIO()
        self.curler.setopt(pycurl.URL, url)
        self.curler.setopt(pycurl.WRITEDATA, self.content_writer)
        try:
            self.curler.perform()
            self.content = self.content_writer.getvalue()
        except:
            print(self.header)
        finally:
            self.content_writer.close()
    
    # NOT TESTED YET
    def post_request(self, url=None, params={}):
        self.header= {}
        if url is None:
            url = self.url
        self.curler.setopt(pycurl.URL, url)
        self.curler.setopt(pycurl.POST, 1)
        self.curler.setopt(pycurl.UPLOAD, 1)
        self.curler.setopt(pycurl.HTTPHEADER, ['application/x-www-form-urlencoded'])
        body = '&'.join([k + '=' + params[k] for k in params])
        self.curler.setopt(pycurl.POSTFIELDSIZE, len(body))
        self.curler.setopt(pycurl.READFUNCTION, StringIO(body).read)
        self.curler.perform()      

    def get_file(self, url, target_path, filename='curl_file'):
        self.header = {}
        self.curler.setopt(pycurl.URL, url)
        self.curler.setopt(pycurl.NOBODY, True)
        self.curler.perform()
        try:
            if ('content-disposition' in self.header):
                filename = os.path.join(target_path, re.search(r'filename=(.+)$', self.header['content-disposition']).groups()[0])
            else:
                filename = os.path.join(target_path, filename)
            self.curler.setopt(pycurl.NOBODY, False)
            with open(filename, 'wb') as filewrite:
                self.curler.setopt(pycurl.WRITEDATA, filewrite)
                self.curler.perform()
        except:
            print(self.header)

    def close(self):
self.curler.close()
