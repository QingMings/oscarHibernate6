package cn.qingmings.oscar;

import org.hibernate.MappingException;
import org.hibernate.dialect.sequence.SequenceSupport;

public  class OscarSequenceSupport implements SequenceSupport {
    public static final SequenceSupport INSTANCE = new OscarSequenceSupport();

    @Override
    public String getSelectSequenceNextValString(String sequenceName) throws MappingException {
        return "nextval('" + sequenceName + "')";
    }

    @Override
    public String getSequenceNextValString(String sequenceName) throws MappingException {
        return "select " + this.getSelectSequenceNextValString(sequenceName);
    }




}
