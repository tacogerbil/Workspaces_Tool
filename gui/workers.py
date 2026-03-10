import sys
import traceback
from PySide6.QtCore import QObject, QRunnable, Signal, Slot

class WorkerSignals(QObject):
    """
    Defines the signals available from a running worker thread.

    Supported signals are:
    - finished: No data
    - error: `tuple` (exctype, value, traceback.format_exc() )
    - result: `object` data returned from processing, anything
    - progress: `int` indicating % progress
    - message: `str` for logging or status bar updates
    """
    finished = Signal()
    error = Signal(tuple)
    result = Signal(object)
    progress = Signal(int)
    message = Signal(str)

class ServiceWorker(QRunnable):
    """
    Worker thread for offloading service layer calls (DB, AWS, SCCM, WMI)
    so they don't block the main PySide6 event loop.

    Requires a function to execute and accepts variable args/kwargs.
    """
    def __init__(self, fn, *args, **kwargs):
        super(ServiceWorker, self).__init__()
        
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()
        
        # Inject signals into the kwargs so the function can emit progress/messages
        # if the function explicitly asks for an event_emitter
        if 'event_emitter' in self.kwargs:
            self.kwargs['event_emitter'] = self.signals

    @Slot()
    def run(self):
        """Initialise the runner function with passed args, kwargs."""
        try:
            # Execute the provided function
            result = self.fn(*self.args, **self.kwargs)
        except Exception:
            # Capture exceptions and emit them to the UI
            traceback.print_exc()
            exctype, value = sys.exc_info()[:2]
            self.signals.error.emit((exctype, value, traceback.format_exc()))
        else:
            self.signals.result.emit(result)  # Return the result of the processing
        finally:
            self.signals.finished.emit()  # Always emit finished
