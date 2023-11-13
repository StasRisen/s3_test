# -*- coding: utf-8 -*-
try:
    import sys
    import os
    
    path = os.path.dirname(sys.executable)
    print('Python directory: ' + path)
    platform = sys.platform
    print('Platform: ' + platform)
    
    if platform == "linux" or platform == "linux2":
        path = 'cd "' + path + '/Scripts/" & '
    elif platform == "darwin":
        path = 'cd "' + path + '" & '
    elif platform == "win32":
        path = 'cd "' + path + '/Scripts/" & '
    
    s = 'n'
    def inst_libs(path):
        k = str(input('\nВведите названия библиотек через пробел. Например:pandas xlrd\nДля выхода наберите \'exit\'\n'))
        if (k == 'exit'):
            return k

        path_out = path + 'pip3 install --user --trusted-host pypi.org --trusted-host files.pythonhosted.org psycopg2-binary ' + k
        print(path_out)
        print(os.system(path_out))
    while(s == 'n'):
        if (inst_libs(path) == 'exit'):
            sys.exit()
        
        s = input('Выйти? (y/n)\n')
        
except Exception as error:
    print(error)
