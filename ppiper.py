import execnet

"""
Pied Piper module allows to charm any python version 
you need  with its subprocess magic. 
Set CPU priority via the nice argument to adjust performance
"""

def call_python(version, module, function, arglist=None, kwargdict=None, nice=None):

    pystr = f"//python=python{version}"
    nicestr = "" if nice is None else f"//nice={nice}"

    # creates and configures a gateway to a Python interpreter. 
    # gateways execute code and exchange data
    # via a bidirectional connection 
    # between the running process and a pool of configurable
    # local/remote interpreters via different thread mechanisms
    # The spec string encodes the target gateway type and configuration.
    # the format is: host-type/key1=value1//key2=value2//...
    # valid types are popen, ssh=hostname, socket=host:port
    gateway = execnet.makegateway(f"popen{pystr}{nicestr}")

    # to make the subprocess wait for arguments
    # it is probably possible to do this in one sweep...
    argstr   = "" if arglist is None else "*channel.receive(),"
    kwargstr = "" if kwargdict is None else "**channel.receive()"
    
    # executes source code in the instantiated subprocess-interpreter
    # by sending it trough a symmetrical channel, retured as object
    # here a simple callback that executes the imported
    # function with the arguments that receive from the host is passed.
    channel = gateway.remote_exec(
    f"""
        from {module} import {function} as f
        channel.send(f({argstr}{kwargstr}))
    """)

    # now hat th channel is open, is possible to send and
    # recieve the data as configured
    if arglist is not None:
        channel.send(arglist)

    if kwargdict is not None:
        channel.send(kwargdict)
    
    # wraps receiver exception status
    try:
        result = channel.receive()
    except channel.RemoteError as e:
        raise Exception(e)
    finally:
        channel.close
    
    return result
