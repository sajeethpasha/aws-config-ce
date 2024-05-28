import pandas as pd


def printD(name,object):
    print('-------------------'+name+'---------------------------------')
    print(object)
    print('-----------------------------------------------------')


# Example DataFrame
df = pd.DataFrame({
    'column1': [1, 2, 3],
    'column2': ['a', 'b', 'c'],
    'column3': ['a3', 'b3', 'c3'],
     'column4': ['a4', 'b4', 'c4'],
})


# dict_default = df.to_dict(orient='list')
# print(dict_default)


printD('default',df.to_dict())

printD('records',df.to_dict(orient='records'))

printD('list',df.to_dict(orient='list'))

printD('split',df.to_dict(orient='split'))


printD('series',df.to_dict(orient='series'))
