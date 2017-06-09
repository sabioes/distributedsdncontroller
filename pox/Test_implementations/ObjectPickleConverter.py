import pickle

class Objectpickle:
  def serialize(self):
    return pickle.dumps(self, protocol=pickle.HIGHEST_PROTOCOL)