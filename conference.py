#!/usr/bin/env python

"""
conference.py -- Conference server-side Python App Engine API;
    uses Google Cloud Endpoints

"""
from datetime import datetime
from datetime import time
from google.appengine.api import taskqueue, urlfetch, memcache
from google.appengine.ext import ndb
from models import BooleanMessage, DateRange, IntegerRange, StringMessage, TimeRange
from models import Conference, ConferenceForm, ConferenceForms, ConferenceQueryForm, ConferenceQueryForms
from models import ConflictException
from models import Profile, ProfileMiniForm, ProfileForm
from models import Session, SessionForm, SessionForms
from models import TeeShirtSize
from protorpc import messages, message_types, remote
from settings import WEB_CLIENT_ID
import endpoints
import utils

# -- Constants definitions -- #
EMAIL_SCOPE = endpoints.EMAIL_SCOPE
API_EXPLORER_CLIENT_ID = endpoints.API_EXPLORER_CLIENT_ID
MEMCACHE_ANNOUNCEMENTS_KEY = "MEMCACHE KEY"
MEMCACHE_SPEAKER_KEY = "MEMCACHE SPEAKER KEY"

DEFAULTS = {
    "city": "Default City",
    "maxAttendees": 0,
    "seatsAvailable": 0,
    "topics": ["Default", "Topic"]
}

OPERATORS = {
    'EQ': '=',
    'GT': '>',
    'GTEQ': '>=',
    'LT': '<',
    'LTEQ': '<=',
    'NE': '!='
}

FIELDS = {
    'CITY': 'city',
    'TOPIC': 'topics',
    'MONTH': 'month',
    'MAX_ATTENDEES': 'maxAttendees',
}

CONF_GET_REQUEST = endpoints.ResourceContainer(
    # Empty request body.
    message_types.VoidMessage,
    # Only 1 URL parameter... :)
    websafeConferenceKey=messages.StringField(1),
)

SESSION_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    websafeSessionKey=messages.StringField(1)
)

CONF_SESSIONS_TYPE_GET_REQUEST = endpoints.ResourceContainer(
    StringMessage,
    websafeConferenceKey=messages.StringField(1)
)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

@endpoints.api(name='conference', version='v1', allowed_client_ids=[WEB_CLIENT_ID, API_EXPLORER_CLIENT_ID],
               scopes=[EMAIL_SCOPE])
class ConferenceApi(remote.Service):
    """
    Conference API v0.1
    """

    # -----------------------------------------------------------
    # - - - Profile objects - - - - - - - - - - - - - - - - - - -
    # -----------------------------------------------------------

    def _copy_profile_to_form(self, profile):
        """
        Copy relevant fields from Profile to ProfileForm.
        """

        profile_form = ProfileForm()
        for field in profile_form.all_fields():
            if hasattr(profile, field.name):
                # convert t-shirt string to Enum; just copy others
                if field.name == 'teeShirtSize':
                    setattr(profile_form, field.name, getattr(TeeShirtSize, getattr(profile, field.name)))
                else:
                    setattr(profile_form, field.name, getattr(profile, field.name))
        profile_form.check_initialized()
        return profile_form

    def _get_profile_from_user(self):
        """
        Return user Profile from datastore, creating new one if non-existent.
        """

        user = endpoints.get_current_user()

        if not user:
            raise endpoints.UnauthorizedException('Authorization required')

        user_id = utils.get_user_id(user)  # We obtain the user id.
        profile_key = ndb.Key(Profile, user_id)  # Then we proceed to generate the key using the user id.

        profile = profile_key.get()  # We look for this profile.

        if not profile:
            profile = Profile(
                key=profile_key,
                displayName=user.nickname(),
                mainEmail=user.email(),
                teeShirtSize=str(TeeShirtSize.NOT_SPECIFIED),
            )

            profile.put()  # This saves the profile in datastore.

        return profile

    def _do_profile(self, save_request=None):
        """
        Get user Profile and return to user, possibly updating it first.
        """
        # get user Profile
        profile = self._get_profile_from_user()

        # if saveProfile(), process user-modifiable fields
        if save_request:
            for field in ('displayName', 'teeShirtSize'):
                if hasattr(save_request, field):
                    val = getattr(save_request, field)
                    if val:
                        setattr(profile, field, str(val))

            profile.put()  # Save the modified profile.

        # return ProfileForm
        return self._copy_profile_to_form(profile)

    @endpoints.method(message_types.VoidMessage, ProfileForm, path='profile', http_method='GET', name='getProfile')
    def get_profile(self, request):
        """
        Return user profile.
        """
        return self._do_profile()

    @endpoints.method(ProfileMiniForm, ProfileForm, path='profile', http_method='POST', name='saveProfile')
    def save_profile(self, request):
        """
        Update & return user profile.
        """
        return self._do_profile(save_request=request)

    # ----------------------------------------------------------
    # - - - Conference objects - - - - - - - - - - - - - - - - -
    # ----------------------------------------------------------
    def _copy_conference_to_form(self, conference, display_name):
        """
        Copy relevant fields from Conference to ConferenceForm.
        """
        conference_form = ConferenceForm()

        for field in conference_form.all_fields():
            if hasattr(conference, field.name):
                # convert Date to date string; just copy others
                if field.name.endswith('Date'):
                    setattr(conference_form, field.name, str(getattr(conference, field.name)))
                else:
                    setattr(conference_form, field.name, getattr(conference, field.name))
            elif field.name == "websafeKey":
                setattr(conference_form, field.name, conference.key.urlsafe())

        if display_name:
            setattr(conference_form, 'organizerDisplayName', display_name)

        conference_form.check_initialized()

        return conference_form

    def _create_conference_object(self, request):
        """
        Create or update Conference object, returning ConferenceForm/request.
        """

        # Pre-load necessary data items
        user = endpoints.get_current_user()

        if not user:
            raise endpoints.UnauthorizedException('Authorization required')

        if not request.name:
            raise endpoints.BadRequestException("Conference 'name' field required")

        user_id = utils.get_user_id(user)

        # Copy ConferenceForm/ProtoRPC Message into dict
        data = {field.name: getattr(request, field.name) for field in request.all_fields()}
        del data['websafeKey']
        del data['organizerDisplayName']

        # Add default values for those missing (both data model & outbound Message)
        for df in DEFAULTS:
            if data[df] in (None, []):
                data[df] = DEFAULTS[df]
                setattr(request, df, DEFAULTS[df])

        # Convert dates from strings to Date objects; set month based on start_date
        if data['startDate']:
            data['startDate'] = datetime.strptime(data['startDate'][:10], "%Y-%m-%d").date()
            data['month'] = data['startDate'].month
        else:
            data['month'] = 0

        if data['endDate']:
            data['endDate'] = datetime.strptime(data['endDate'][:10], "%Y-%m-%d").date()

        # Set seatsAvailable to be same as maxAttendees on creation
        # Both for data model & outbound Message
        if data["maxAttendees"] > 0:
            data["seatsAvailable"] = data["maxAttendees"]
            setattr(request, "seatsAvailable", data["maxAttendees"])

        # Make Profile Key from user ID
        profile_key = ndb.Key(Profile, user_id)
        # Allocate new Conference ID with Profile key as parent/ancestor
        conference_id = Conference.allocate_ids(size=1, parent=profile_key)[0]
        # Make Conference key from ID
        conference_key = ndb.Key(Conference, conference_id, parent=profile_key)
        data['key'] = conference_key
        data['organizerUserId'] = request.organizerUserId = user_id

        # Create Conference & return (modified) ConferenceForm
        Conference(**data).put()
        taskqueue.add(params={'email': user.email(),
                              'conferenceInfo': repr(request)
                              },
                      url='/tasks/send_confirmation_email'
                      )

        return request

    @endpoints.method(ConferenceForm, ConferenceForm, path='conference', http_method='POST', name='createConference')
    def create_conference(self, request):
        """
        Create new conference.
        """
        return self._create_conference_object(request)

    def _get_query(self, request):
        """
        Return formatted query from the submitted filters.
        """
        query = Conference.query()
        inequality_filter, filters = self._format_filters(request.filters)

        # If exists, sort on inequality filter first
        if not inequality_filter:
            query = query.order(Conference.name)
        else:
            query = query.order(ndb.GenericProperty(inequality_filter))
            query = query.order(Conference.name)

        for filtr in filters:
            if filtr["field"] in ["month", "maxAttendees"]:
                filtr["value"] = int(filtr["value"])
            formatted_query = ndb.query.FilterNode(filtr["field"], filtr["operator"], filtr["value"])
            query = query.filter(formatted_query)
        return query

    def _format_filters(self, filters):
        """
        Parse, check validity and format user supplied filters.
        """
        formatted_filters = []
        inequality_field = None

        for f in filters:
            filters = {field.name: getattr(f, field.name) for field in f.all_fields()}

            try:
                filters["field"] = FIELDS[filters["field"]]
                filters["operator"] = OPERATORS[filters["operator"]]
            except KeyError:
                raise endpoints.BadRequestException("Filter contains invalid field or operator.")

            # Every operation except "=" is an inequality
            if filters["operator"] != "=":
                # Check if inequality operation has been used in previous filters
                # Disallow the filter if inequality was performed on a different field before
                # Track the field on which the inequality operation is performed
                if inequality_field and inequality_field != filters["field"]:
                    raise endpoints.BadRequestException("Inequality filter is allowed on only one field.")
                else:
                    inequality_field = filters["field"]

            formatted_filters.append(filters)
        return inequality_field, formatted_filters

    @endpoints.method(ConferenceQueryForms, ConferenceForms, path='queryConferences', http_method='POST',
                      name='queryConferences')
    def query_conferences(self, request):
        """
        Query for conferences.
        """
        conferences = self._get_query(request)

        # Return individual ConferenceForm object per Conference
        return ConferenceForms(
            items=[self._copy_conference_to_form(conf, "") for conf in conferences]
        )

    @endpoints.method(message_types.VoidMessage, ConferenceForms, path='getConferencesCreated', http_method='POST',
                      name='getConferencesCreated')
    def get_conferences_created(self, request):
        """
        Return conferences created by user.
        """
        # Make sure user is authorized
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')

        # Make profile key
        profile_key = ndb.Key(Profile, utils.get_user_id(user))
        # Create ancestor query for this user
        conferences = Conference.query(ancestor=profile_key)
        # Get the user profile and display name
        prof = profile_key.get()
        display_name = getattr(prof, 'displayName')
        # Return set of ConferenceForm objects per Conference
        return ConferenceForms(
            items=[self._copy_conference_to_form(conf, display_name) for conf in conferences]
        )

    @endpoints.method(CONF_GET_REQUEST, ConferenceForm, path='conference/{websafeConferenceKey}', http_method='GET',
                      name='getConference')
    def get_conference(self, request):
        """
        Return requested conference (by websafeConferenceKey).
        """
        # Get Conference object from request; bail if not found
        conference = ndb.Key(urlsafe=request.websafeConferenceKey).get()

        if not conference:
            raise endpoints.NotFoundException('No conference found with key: %s' % request.websafeConferenceKey)

        profile = conference.key.parent().get()

        # Return ConferenceForm
        return self._copy_conference_to_form(conference, getattr(profile, 'displayName'))

    # ----------------------------------------------------------
    # - - - Sessions - - - - - - - - - - - - - - - - - - - - - -
    # ----------------------------------------------------------

    def _create_session_object(self, request):
        # TODO Implement.
        user = endpoints.get_current_user()

        if not user:
            raise endpoints.UnauthorizedException('Authorization required')

        if not request.conferenceWebsafeKey:
            raise endpoints.BadRequestException("Session 'conferenceWebsafeKey' required")

        if not request.name:
            raise endpoints.BadRequestException("Session 'name' required")

        data = {field.name: getattr(request, field.name) for field in request.all_fields()}
        del data['sessionWebsafeKey']

        conference = ndb.Key(urlsafe=request.conferenceWebsafeKey).get()
        session_id = Session.allocate_ids(size=1, parent=conference.key)[0]
        session_key = ndb.Key(Session, session_id, parent=conference.key)

        # Process defaults values
        # If there's no date, then we use the start date from the conference.
        if not data['date']:
            data['date'] = conference.startDate
            setattr(request, 'date', datetime.strftime(data['date'], "%Y-%m-%d"))
        else:
            data['date'] = datetime.strptime(data['date'][:10], "%Y-%m-%d").date()

        if not data['startTime']:
            data['startTime'] = datetime.now().time()
            setattr(request, 'startTime', time.strftime(data['startTime'], "%H:%M"))
        else:
            data['startTime'] = datetime.strptime(data['startTime'], "%H:%M").time()

        if not data['duration']:
            data['duration'] = 60
            setattr(request, "duration", 60)

        if not data['speaker']:
            profile = self._get_profile_from_user()
            data['speaker'] = profile.displayName
            setattr(request, 'speaker', data['speaker'])

        if not data['highlights']:
            data['highlights'] = "Amazing session! Don't miss it."
            setattr(request, 'highlights', data['highlights'])

        data['key'] = session_key

        # Get all sessions in this conference.
        sessions = Session.query(ancestor=conference.key)

        # If there's more than one session with this speaker, then we'll cache the speaker and sessions names.
        if [s.speaker for s in sessions].count(data['speaker']) > 1:
            # TODO Put it in cache.
            pass

        Session(**data).put()

        return request

    def _copy_session_to_form(self, session):
        session_form = SessionForm()

        for field in session_form.all_fields():
            if hasattr(session, field.name):
                # convert Time to time string; just copy others
                if field.name.endswith('Time') or field.name.endswith("date"):
                    setattr(session_form, field.name, str(getattr(session, field.name)))
                else:
                    setattr(session_form, field.name, getattr(session, field.name))

        setattr(session_form, "sessionWebsafeKey", session.key.urlsafe())

        session_form.check_initialized()

        return session_form

    @endpoints.method(SessionForm, SessionForm, path="session", http_method='POST', name='createSession')
    def create_session(self, request):
        return self._create_session_object(request)

    @endpoints.method(CONF_GET_REQUEST, SessionForms, path='conference/{websafeConferenceKey}/sessions',
                      http_method='GET', name='getConferenceSessions')
    def get_conference_sessions(self, request):

        # TODO Must perform validations.
        # Retrieve the conference key.
        conference_key = ndb.Key(urlsafe=request.websafeConferenceKey)

        # Look fot all the sessions that belongs to this conference.
        sessions = Session.query(ancestor=conference_key)

        return SessionForms(
            items=[self._copy_session_to_form(s) for s in sessions]
        )

    @endpoints.method(CONF_SESSIONS_TYPE_GET_REQUEST, SessionForms, path='sessionsByType/{websafeConferenceKey}',
                      http_method='POST', name='getConferenceSessionsByType')
    def get_conference_sessions_by_type(self, request):

        # TODO Must perform validations.
        # Retrieve the conference key.
        conference_key = ndb.Key(urlsafe=request.websafeConferenceKey)

        query = Session.query(Session.typeOfSession == request.data, ancestor=conference_key)
        # query = Session.query(ancestor=conference_key)
        # query = query.filter(ndb.query.FilterNode("typeOfSession", "=", request.typeOfSession))

        return SessionForms(
            items=[self._copy_session_to_form(s) for s in query]
        )

    @endpoints.method(StringMessage, SessionForms, path='sessionsBySpeaker', http_method='POST',
                      name='getSessionsBySpeaker')
    def get_sessions_by_speaker(self, request):
        query = Session.query(Session.speaker == request.data)

        return SessionForms(
            items=[self._copy_session_to_form(s) for s in query]
        )

    @endpoints.method(IntegerRange, SessionForms, path='sessionsByDuration', http_method='POST',
                      name='sessionsByDuration')
    def get_sessions_by_duration(self, request):
        # Gets all sessions.
        query = Session.query()

        if request.min:
            if request.min < 0:
                raise endpoints.BadRequestException('"min" has to be a positive number.')

            # We are sure request.min is present and is valid.
            query = query.filter(Session.duration >= request.min)

        if request.max:
            if request.max < 0:
                raise endpoints.BadRequestException('"max" has to be a positive number.')

            # We are sure request.max is present and is valid.
            query = query.filter(Session.duration <= request.max)

        if request.min and request.max and request.min >= request.max:
                raise endpoints.BadRequestException('"min" must be lesser than "max"')

        query = query.order(Session.duration)

        return SessionForms(
            items=[self._copy_session_to_form(s) for s in query]
        )

    @endpoints.method(DateRange, SessionForms, path='sessionsByDate', http_method='POST',
                      name='sessionsByDate')
    def get_sessions_by_date(self, request):
        # Gets all sessions.
        query = Session.query()

        min_date = None
        max_date = None

        if request.min:
            min_date = datetime.strptime(request.min[:10], "%Y-%m-%d").date()

            # We are sure request.min is present and is valid.
            query = query.filter(Session.date >= min_date)

        if request.max:
            max_date = datetime.strptime(request.max[:10], "%Y-%m-%d").date()

            # We are sure request.max is present and is valid.
            query = query.filter(Session.date <= max_date)

        if min_date and max_date and min_date >= max_date:
                raise endpoints.BadRequestException('"min" must be lesser than "max"')

        query = query.order(Session.date)

        return SessionForms(
            items=[self._copy_session_to_form(s) for s in query]
        )

    @endpoints.method(TimeRange, SessionForms, path='sessionsByStartTime', http_method='POST',
                      name='sessionsByStartTime')
    def get_sessions_by_start_time(self, request):
        # Gets all sessions.
        query = Session.query()

        min_time = None
        max_time = None

        if request.min:
            min_time = datetime.strptime(request.min, "%H:%M").time()

            # We are sure request.min is present and is valid.
            query = query.filter(Session.startTime >= min_time)

        if request.max:
            max_time = datetime.strptime(request.max, "%H:%M").time()

            # We are sure request.max is present and is valid.
            query = query.filter(Session.startTime <= max_time)

        if min_time and max_time and min_time >= max_time:
                raise endpoints.BadRequestException('"min" must be lesser than "max"')

        query = query.order(Session.startTime)

        return SessionForms(
            items=[self._copy_session_to_form(s) for s in query]
        )

    # ----------------------------------------------------------
    # - - - Wishlist - - - - - - - - - - - - - - - - - - - - - -
    # ----------------------------------------------------------

    @endpoints.method(SESSION_GET_REQUEST, BooleanMessage, path='users/sessions/{websafeSessionKey}', http_method='POST',
               name='addSessionToWishlist')
    def add_session_to_wishlist(self, request):

        profile = self._get_profile_from_user()  # get user Profile

        session_key = request.websafeSessionKey
        session = ndb.Key(urlsafe=session_key).get()

        if not session:
            raise endpoints.NotFoundException('No session found with key: %s' % session_key)

        if session_key in profile.sessionsKeysWishlist:
            raise ConflictException('You already have this session in your wishlist.')

        profile.sessionsKeysWishlist.append(session_key)
        profile.put()

        return BooleanMessage(data=True)

    @endpoints.method(message_types.VoidMessage, SessionForms, path='sessions/wishlist', http_method='GET',
                      name='getSessionsInWishlist')
    def get_sessions_in_wishlist(self, request):
        profile = self._get_profile_from_user()

        sessions_wishlist = profile.sessionsKeysWishlist

        sessions_keys = [ndb.Key(urlsafe=key) for key in sessions_wishlist]

        sessions = ndb.get_multi(sessions_keys)

        return SessionForms(items=[self._copy_session_to_form(s) for s in sessions])

    @endpoints.method(SESSION_GET_REQUEST, BooleanMessage, path='users/sessions/{websafeSessionKey}/delete', http_method='POST',
               name='deleteSessionInWishlist')
    def delete_session_in_wishlist(self, request):

        profile = self._get_profile_from_user()  # get user Profile

        session_key = request.websafeSessionKey
        session = ndb.Key(urlsafe=session_key).get()

        if not session:
            raise endpoints.NotFoundException('No session found with key: %s' % session_key)

        if session_key not in profile.sessionsKeysWishlist:
            return BooleanMessage(data=False)

        profile.sessionsKeysWishlist.remove(session_key)
        profile.put()

        return BooleanMessage(data=True)

    # - - - Registration - - - - - - - - - - - - - - - - - - - -

    # xg stands for Cross-Group. It means that changes made in this function to more than one entity are transactional
    @ndb.transactional(xg=True)
    def _conference_registration(self, request, reg=True):
        """
        Register or unregister user for selected conference.
        """
        return_value = None
        profile = self._get_profile_from_user()  # get user Profile

        # Check if conf exists given websafeConfKey
        # Get conference; check that it exists
        wsck = request.websafeConferenceKey
        conference = ndb.Key(urlsafe=wsck).get()
        if not conference:
            raise endpoints.NotFoundException('No conference found with key: %s' % wsck)

        # Register
        if reg:
            # Check if user already registered otherwise add
            if wsck in profile.conferenceKeysToAttend:
                raise ConflictException("You have already registered for this conference")

            # Check if seats available
            if conference.seatsAvailable <= 0:
                raise ConflictException("There are no seats available.")

            # Register user, take away one seat
            profile.conferenceKeysToAttend.append(wsck)
            conference.seatsAvailable -= 1
            return_value = True

        # Unregister
        else:
            # Check if user already registered
            if wsck in profile.conferenceKeysToAttend:

                # Unregister user, add back one seat
                profile.conferenceKeysToAttend.remove(wsck)
                conference.seatsAvailable += 1
                return_value = True
            else:
                return_value = False

        # Write things back to the datastore & return
        profile.put()
        conference.put()
        return BooleanMessage(data=return_value)

    @endpoints.method(CONF_GET_REQUEST, BooleanMessage, path='conference/{websafeConferenceKey}', http_method='POST',
                      name='registerForConference')
    def register_for_conference(self, request):
        """
        Register user for selected conference.
        """
        return self._conference_registration(request)

    @endpoints.method(message_types.VoidMessage, ConferenceForms, path='conferences/attending', http_method='GET',
                      name='getConferencesToAttend')
    def get_conferences_to_attend(self, request):
        """
        Get list of conferences that user has registered for.
        """

        # Step 1: get user profile
        profile = self._get_profile_from_user()

        # Step 2: get conferenceKeysToAttend from profile.
        conferences_to_attend = profile.conferenceKeysToAttend

        # To make a ndb key from websafe key you can use:
        conferences_keys = [ndb.Key(urlsafe=key) for key in conferences_to_attend]

        # ndb.Key(urlsafe=my_websafe_key_string)
        # Step 3: fetch conferences from datastore.
        # Use get_multi(array_of_keys) to fetch all keys at once.
        # Do not fetch them one by one!
        conferences = ndb.get_multi(conferences_keys)

        # Return set of ConferenceForm objects per Conference
        return ConferenceForms(items=[self._copy_conference_to_form(conf, "") for conf in conferences])

    # - - - Announcements - - - - - - - - - - - - - - - - - - - -

    @staticmethod
    def _cache_announcement():
        """
        Create Announcement & assign to memcache; used by
        memcache cron job & putAnnouncement().
        """
        conferences = Conference.query(ndb.AND(Conference.seatsAvailable <= 5, Conference.seatsAvailable > 0)). \
            fetch(projection=[Conference.name])

        if conferences:
            # If there are almost sold out conferences,
            # format announcement and set it in memcache
            announcement = '%s %s' % (
                'Last chance to attend! The following conferences '
                'are nearly sold out:',
                ', '.join(conf.name for conf in conferences))
            memcache.set(MEMCACHE_ANNOUNCEMENTS_KEY, announcement)
        else:
            # If there are no sold out conferences,
            # delete the memcache announcements entry
            announcement = ""
            memcache.delete(MEMCACHE_ANNOUNCEMENTS_KEY)

        return announcement

    @endpoints.method(message_types.VoidMessage, StringMessage, path='conference/announcement/get', http_method='GET',
                      name='getAnnouncement')
    def get_announcement(self, request):
        """
        Return Announcement from memcache.
        """

        # return an existing announcement from Memcache or an empty string.
        announcement = ""
        return StringMessage(data=announcement)


# Registers API
api = endpoints.api_server([ConferenceApi])
