#!/usr/bin/env python

from conference import ConferenceApi
from google.appengine.api import app_identity
from google.appengine.api import mail
import webapp2


class SetAnnouncementHandler(webapp2.RequestHandler):
    def get(self):
        """
        Set Announcement in Memcache.
        """

        ConferenceApi._cache_announcement()


class SendConfirmationEmailHandler(webapp2.RequestHandler):
    def post(self):
        """
        Send email confirming Conference creation.
        """
        mail.send_mail(
            'noreply@%s.appspotmail.com' % (
                app_identity.get_application_id()),     # from
            self.request.get('email'),                  # to
            'You created a new Conference!',            # subject
            'Hi, you have created a following '         # body
            'conference:\r\n\r\n%s' % self.request.get(
                'conferenceInfo')
        )


app = webapp2.WSGIApplication([
    ('/crons/set_announcement', SetAnnouncementHandler),
    ('/tasks/send_confirmation_email', SendConfirmationEmailHandler),
], debug=True)