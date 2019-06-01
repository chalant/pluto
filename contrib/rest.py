def decorator(func):
    def inner(instance):
        print("decorated")
        func(instance)
    return inner

class Test(object):
    def say_hello(self):
        print('Hello!!!')

class SubTest(Test):
    def __init__(self):
        self._name = 'YO'
    @decorator
    def say_hello(self):
        print('Hello!!!')

t1 = Test()
t2 = SubTest()

print('test')
t1.say_hello()
print('subtest')
t2.say_hello()