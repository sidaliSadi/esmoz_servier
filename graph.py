
class Node(object):
    def __init__(self, name, ctype=None, cdate=None, size=None):
        self.name = name
        self.children = []
        self.date = cdate
        self.type = ctype

    def child(self, cname, ctype, cdate, size=None):
        child_found = [c for c in self.children if c.name == cname]
        if not child_found:
            _child = Node(cname,ctype, cdate, size)
            self.children.append(_child)
        else:
            _child = child_found[0]
        return _child

    def as_dict(self):
        if self.type == 'pubmed':
            res = {'pubmed': self.name, 'date' : self.date}
        elif self.type == 'journal':
            res = {'journal': self.name, 'date' : self.date}
        elif self.type == 'clinic':
            res = {'clinical_trial': self.name, 'date' : self.date}
        else:
            res = {'name': self.name}
        
        res['children'] = [c.as_dict() for c in self.children]
        return res

    