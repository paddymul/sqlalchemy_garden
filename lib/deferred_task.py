import pdb
from conf import config 

from sqlalchemy import event
from sqlalchemy.orm import mapper
from sqlalchemy.util.langhelpers import symbol

from sqlalchemy.sql.expression import Select
NO_VALUE = symbol("NO_VALUE")

from lib.db_connection import db_session

try:
    from celery.task import task as ctask
    CELERY_INSTALLED = True
except ImportError, e:
    print "You don't have celery installed, running in syncronous mode"
    def ctask(actual_func, *args, **kwargs):
        return actual_func
    CELERY_INSTALLED = False


class deferred(object):
    def __init__(self, trigger_condition,
                 change_field=False,  
                 before_predicate=lambda obj: True,
                 run_predicate=lambda obj: True):
        """The arguments to init are the arguments that are available
        to the @deferred decorator
        """
        self.before_predicate = before_predicate
        self.trigger_condition = trigger_condition
        self.run_predicate = run_predicate
        self.change_field = change_field

    def make_task(self):
        """ This function makes the celery task object.  It is run
        both on the enqueuing side and on the daemon side.  On the
        enqueueing side it is necessary to run this function in order
        to create the task_name that will be enqueued.  On the daemon
        side this function is run to register this task function with
        the task_name, and to create this function

        """
        tn, fn  = self.TableObject.__tablename__, self.actual_task.func_name
        self.task_name =  tn + "_" + fn + "_" +self.trigger_condition

        def _task_wrapper(target_id):
            """ 
            this is the first command that is run by the celery-daemon
            """
            with db_session(config.DB_URI) as s:
                p = s.query(self.TableObject).filter_by(id=target_id)[0]
                if self.run_predicate(p):
                    # run the originally decorated function
                    self.actual_task(p, s)
                    s.add(p)
                    s.commit()
        self._task_wrapper = _task_wrapper
        self.celery_task = ctask(_task_wrapper, name=self.task_name)

    def __call__(self, func):
        """ we don't modify the function itself, we just set the
        deferred flag on it, so that our after_mapper_config hook can
        recognize it """

        self.actual_task = func
        func.deferred = True
        func.deferred_obj = self
        return func

    def setup_sql_listener(self, TableObject):
        """
        this function is run once a mapper is configured, at this
        point we have access not only to the property name, but to the
        TableObject that the property is a member of

        """
        self.TableObject = TableObject
        self.make_task()
        
        changed_targets = []
            
        def field_change_callback(target, value, oldvalue, initiator):
            print self.task_name
            if oldvalue == NO_VALUE:
                return
            elif value != oldvalue:
                if target.id not in changed_targets:
                    changed_targets.append(target.id)

        if self.change_field:
            event.listen(
                getattr(TableObject, self.change_field), "set",
                field_change_callback, retval=False)
        import pdb
        def run_it(target_id):
            if config.CELERY_SYNCHRONOUS or not CELERY_INSTALLED:
                self._task_wrapper(target_id)
            else:
                self.celery_task.delay(target_id)

        def make_conn_doit(target):
            target_id = target.id
            if self.before_predicate(target):
                if self.change_field:
                    #pdb.set_trace()
                    if target_id in changed_targets:
                        changed_targets.remove(target_id)
                        return lambda: run_it(target_id)
                else:
                    return lambda: run_it(target_id)
            return lambda: 5
        def trigger_f(mapper, connection, target):
            print self.task_name
            eng = connection.engine
            if not hasattr(eng, "do_it_listener"):
                event.listen(
                    eng, "after_execute", conn_after_execute)
                eng.do_it_listener = True
                eng.do_its = []
            doit = make_conn_doit(target)
            doit.target_id = target.id
            doit.task_name = self.task_name
            eng.do_its.append(doit)

            

        
        # here we register the listener for the @deferred trigger_condition
        event.listen(TableObject, self.trigger_condition, trigger_f)

def conn_after_execute(conn, clauseelement, multiparams, params, result):
    eng = conn.engine
    l_doits = len(eng.do_its)
    if isinstance(clauseelement, Select) or l_doits == 0:
        return
    

    
    local_doits = []
    for do_it in eng.do_its:
        print do_it.task_name, do_it.target_id
        local_doits.append(do_it)
    print clauseelement
    eng.do_its = []
    for do_it in local_doits:
        do_it()
                                 

@event.listens_for(mapper, "mapper_configured")
def _setup_deferred_properties(mapper, class_):
    """Listen for finished mappers and apply DeferredProp
    configurations."""
    for attr_name in class_.__dict__.keys():
        attr  = getattr(mapper.class_, attr_name)
        if hasattr(attr, "deferred"):
            deferred_obj  = getattr(attr, "deferred_obj")
            deferred_obj.setup_sql_listener(mapper.class_)

"""
Order of operations:
1. deferred.__init__ called when interpreter sees the @deferred line

2. deferred.__call__ called when the interpreter finishes the function
   definition to which the decorator applies.  __call__ recieves a
   reference to the function to be decorated

3. _setup_deferred_properties runs next. This function runs once all
   the models have been configured.

4. deferred.setup_sql_listener runs.  This function registers the
   proper listeners for the task.

5. deferred.make_task runs.  This creates the celery_task object and
   gives it a name.  The celery_task object wraps the originally
   decorated function, setting it up to be able to be run on the daemon
   side.

-----
The above functions always run, whether in the daemon, or in an enqueing context. 


The following functions only run at enqueing time.

6. setup the listener on the enigne "after_execute event", this will
trigger 7 if the proper pre conditions are met.


7. If and only if the trigger_condition is met,
   deferred.setup_sql_listener[trigger_f] is called by the event
   callback.  the trigger_f's only job is to enqueue the actual_task,
   that was created by deferred.make_task.  Trigger_f checks that
   before_predicate returns True when passed the model_object instace.
   Trigger_f also checks that the change_field being watched for this
   object was actually changed (this prevents infinite loops when
   watching the "after_update" event".

-----
The following functions only run on the daemon side

8. deferred.actual_task is called with the instance_id of the enqueue
   object.  It re-instatiates the object, then calls after_predicate,
   passing in the instance.  if after_predicate returns_true, the
   decorated function is run, recieving an instance of the object and
   the session object as arguments.

"""

