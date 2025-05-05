"""
====================================================================================
File                :   constants.py
Description         :   constants config
Author              :   Murugesan G
Date Created        :   Feb 7th 2023
Last Modified BY    :   Murugesan G
Date Modified       :   Feb 9th 2023
====================================================================================
"""

DELETED_ERROR = "Check User name to delete"
CREATING_ERROR = "Unable to create "
UPDATE_ERROR = "Unable to update"
CHANGE_ERROR = "Unable to change"
RESET_ERROR = "Unable to reset"
TOKEN = 'token'
DATE_FORMAT = '%d-%m-%Y'
DIRECTORY_SUCCESS = "../logs/"

SUCESS_FILENAME = DIRECTORY_SUCCESS + '/iAnalytic-{}.log'

FORMAT_LOGGING = '%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s:%(message)s'
UTC_DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%SZ"
DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

ENCRYPT_ALGO = 'HS256'
SECRET_KEY = b'!@#LivNSense123'
MESSAGE_KEY = "message"
UTF8_FORMAT = "utf-8"
STATUS_KEY = "status"
DB_ERROR = "No connection to database"
RECORDS = "records"
EXCEPTION_CAUSE = "Something else happend {}"
CASSANDRA_DB_KEY = 'cassandra_db'
TOKEN_KEY = "token"
GET_REQUEST = "GET"
POST_REQUEST = "POST"
DELETE_REQUEST = "DELETE"
PUT_REQUEST = "PUT"
METHOD_NOT_ALLOWED = "Method is not allowed"


# ############################# DATABASE TABLE CONSTANTS
EQUIPMENT_MASTER_TABLE = "equipment_master"
DT_STABILITY_TABLE = "dt_stability_index"
DT_RESULT_TABLE = "dt_result_table"

"""
                User Management
_____________======================_______________
"""
DEFAULT_NOTIFICATION_VIEW = "Default Notification View Timer Period"
MAXIMUM_VALUES_NOTIFICATION_DOWNLOAD_TIME_PERIOD = "Maximum Values Notification Download Time Period"
SETTING = 'setting'
VALUE = 'value'
SECTION = "section"
FEATURE = "feature"
SUPER_ADMIN = "Super Admin"
ADMIN = "Admin"
NON_ADMIN = "Non Admin"
LOGIN_ID = "login_id"
USERNAME_KEY = "username"
PARALLEL_SESSIONS = 'parallel_sessions'
STANDARD_USER = 'standard_user'
ADMIN_USER = 'admin_user'
NAME = "name"
USERPASSWORD_KEY = "password"
USEREMAIL_KEY = "email_id"
DESIGNATION = "designation"
USER_TYPE = "user_type"
EAMIL_NOTIFICATION = "notifications"
USERFUTUREPASSWORD_KEY = "newpassword"
PASSWORD_EXPIRY_PERIOD = "Password Expiry Period"
USERFIRSTNAME_KEY = "firstname"
USERMIDDLENAME_KEY = "middlename"
USERLASTNAME_KEY = "lastname"
AGE_KEY = "age"
GENDER_KEY = "gender"
PHONE_KEY = "phone_no"
ADDRESS_KEY = "address"
ROLE_KEY = "role"
SALT_KEY = "salt"
COMMISSIONSTATUS_KEY = "commission_status"
USERID_KEY = "id"
STATUS_VALUE = "This user is decommissioned"

LOGGEDINUSERNAME_KEY = "loggedin_username"
LOGGEDINUSEREMAIL_KEY = "loggedin_useremail"
LOGGEDINUSERID_KEY = "loggedin_userid"
LOGGEDINUSERROLE_KEY = "loggedin_userrole"

PASSWORD_WRONG = "Password is not correct"
USERNAME_NOT_REGISTERED = "Username is not registered with us"
EMAILID_NOT_REGISTERED = "Email ID is not registered with us"
ROLE_PERMISSION = "This role not allow for Unit level"

CREATED_SUCCESSFULLY = "Created Successfully"
UPDATED_SUCCESSFULLY = "Updated Successfully"
RESET_SUCCESSFULLY = "Reset Successfully"
DELETED_SUCCESSFULLY = "Deleted Successfully"
CHANGED_SUCCESSFULLY = "Changed Successfully "
DOWNLOADED_SUCCESSFULLY = "Downloaded Successfully"
UPLOADED_SUCCESSFULLY = "Uploaded Successfully"
ERROR_TYPE_NOT_ALLOWED = "This type is not allowed"

""" =============================================== """

HTTP_AUTHORIZATION_TOKEN = "HTTP_AUTHORIZATION"

TAG_NAME_REQUEST = "tag_name"
START_DATE_REQUEST = "start_date"
END_DATE_REQUEST = "end_date"
FEATURE_REQUEST = "feature"
FILES_NAME_REQUEST = "files"
TYPE_REQUEST = "type"
SECONDARY_TAG_REQUEST = "secondary_tag"
ONLINE_DRUM_REQUEST = "online_drum"
DAILY_AVG_REQUEST = "is_daily_avg_value"
MULTILINE_REQUEST = "is_multiline"
MOVING_AVG_REQUEST = "is_moving_avg_value"
PRIMARY_TAG = "primary_tag"


# REQUEST METHOD
GET = 'GET'
POST = 'POST'
PUT = 'PUT'
DELETE = 'DELETE'

# messages
ACTION = 'action'
LOGOUT = 'logout'
LOGGED_OUT_SUCCESS = {"message": "logged out successfully"}
LOGGED_OUT_FAILURE = {"message": "user already logged out successfully"}
WRONG_PASSWORD = 'Password is not correct'
USER_NOT_EXIST = 'Username is not registered with us'
SESSION_EXPIRY = 900


# REQUEST BODY KEYS
REQ_USER_ID = 'user_id'
REQ_USERNAME = 'username'
REQ_PASSWORD = 'password'
REQ_ROLE_ID = 'role_id'
REQ_START_DATE = 'start_date'
REQ_END_DATE = 'end_date'
REQ_ID = 'id'
TICKET_ID = 'ticket_id'
EMP_ID = 'employee_id'
ASSESSMENT_ID = 'assessment_id'
REQ_U_NAME = 'username'
REQ_ACTION = 'action'
UNMAPPED = 'unmapped'
MAPPED = 'mapped'
REQ_CAMERA_POSITION = 'cam'