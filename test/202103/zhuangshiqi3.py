import functools


user,passwd = 'sun' ,'123'
def auth(auth_type):
    print("auth func:",auth_type)
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args,**kwargs):
            print('wrapper func args:',*args,**kwargs)
            if auth_type == 'local':
                username = input('Username:').strip()
                password = input("Password:").strip()
                if user == username and passwd == password:
                    print("\033[32;1mUser has passed authentication\033[0m")
                    res = func(*args, **kwargs)
                    print("--after authentication--")
                    return res
                else:
                    exit("\033[31;1mInvalid username or password\033[0m")
            elif auth_type == 'ldap':
                res = func(*args, **kwargs)
                print("搞毛线ldap,不会。。。。")
                return res

        return wrapper
    return decorator

def index():
    print("welcome to index page")

@auth(auth_type='local')
def home():
    print("welcome to home page")
    return 'from home'

@auth(auth_type='ldap')
def bbs():
    print("welcome to bbs page")


index()
print(home())  #wrapper
bbs()

