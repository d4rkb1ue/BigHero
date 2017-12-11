#include "qe.h"

Filter::Filter(Iterator *input, const Condition &condition)
    : input(input),
      condition(condition)
{
    getAttributes(attrs);
}

RC Filter::getNextTuple(void *data)
{
    if (input->getNextTuple(data) == -1)
    {
        return -1;
    }
    if (condition.op == NO_OP)
    {
        return 0;
    }
    Record record(attrs, static_cast<const char *>(data));
    // cerr << record.toString(attrs) << endl;
    // return -1;
    record.getAttribute(attrs, condition.lhsAttr, buffer);
    int res = compareTo(condition.rhsValue.type, buffer, static_cast<char *>(condition.rhsValue.data));
    // cerr << record.toString(attrs) << endl;
    // cerr << "result = " << res << endl;
    switch (condition.op)
    {
    case EQ_OP:
    {
        if (res == 0)
        {
            return 0;
        }
        break;
    }
    case LT_OP:
    {
        if (res < 0)
        {
            return 0;
        }
        break;
    }
    case LE_OP:
    {
        if (res <= 0)
        {
            return 0;
        }
        break;
    }
    case GT_OP:
    {
        if (res > 0)
        {
            return 0;
        }
        break;
    }
    case GE_OP:
    {
        if (res >= 0)
        {
            return 0;
        }
        break;
    }
    case NE_OP:
    {
        if (res != 0)
        {
            return 0;
        }
        break;
    }
    case NO_OP:
    {
        return 0;
    }
    }
    return getNextTuple(data);
}

void Filter::getAttributes(vector<Attribute> &attrs) const
{
    return input->getAttributes(attrs);
}

int Filter::compareTo(AttrType type, char *value, char *thatVal)
{
    switch (type)
    {
    case TypeInt:
    {
        int int_this = 0, int_that = 0;
        memcpy(&int_this, value, sizeof(int));
        memcpy(&int_that, thatVal, sizeof(int));
        // cerr << "comparing " << int_this << " ? " << int_that << endl;
        return int_this - int_that;
    }
    case TypeReal:
    {
        float f_this = 0, f_that = 0;
        memcpy(&f_this, value, sizeof(float));
        memcpy(&f_that, thatVal, sizeof(float));
        // cerr << "comparing " << f_this << " ? " << f_that << endl;
        if (f_this - f_that < 0.001 && f_that - f_this < 0.001)
        {
            return 0;
        }
        return ((f_this - f_that) < 0.0 ? -1 : 1);
    }
    case TypeVarChar:
    {
        unsigned size = Utils::getVCSizeWithHead(value);
        unsigned thatSize = Utils::getVCSizeWithHead(thatVal);

        // empty string
        if (size == sizeof(unsigned))
        {
            return thatSize == sizeof(unsigned) ? 0 : -1;
        }
        if (thatSize == sizeof(unsigned))
        {
            return 1;
        }

        // + '\0'
        char _c[size - sizeof(unsigned) + 1];
        memcpy(_c, value + sizeof(unsigned), size - sizeof(unsigned));
        _c[size - sizeof(unsigned)] = '\0';
        string _s(_c);

        char _c_that[thatSize - sizeof(unsigned) + 1];
        memcpy(_c_that, thatVal + sizeof(unsigned), thatSize - sizeof(unsigned));
        _c_that[thatSize - sizeof(unsigned)] = '\0';
        string _s_that(_c_that);

        // cerr << "comparing \"" << _s << "\" ? \"" << _s_that << "\"" << endl;
        return _s.compare(_s_that);
    }
    }
    return 0;
}