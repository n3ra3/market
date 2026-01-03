import importlib.util, os
print('cwd', os.getcwd())
print('sys.path[0]=', importlib.util.find_spec(''))
print('find bot:', importlib.util.find_spec('bot'))
print('find bot.config:', importlib.util.find_spec('bot.config'))
try:
    import bot
    print('bot module', getattr(bot,'__file__',None), getattr(bot,'__path__',None))
except Exception as e:
    print('import bot failed', e)
