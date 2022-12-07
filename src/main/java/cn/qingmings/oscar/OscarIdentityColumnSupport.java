package cn.qingmings.oscar;

import org.hibernate.dialect.identity.IdentityColumnSupportImpl;

class OscarIdentityColumnSupport extends IdentityColumnSupportImpl {
            OscarIdentityColumnSupport() {
            }

            public boolean supportsIdentityColumns() {
                return true;
            }

            public String getIdentitySelectString(String var1, String var2, int var3) {
                return "select currval('" + var1 + '_' + var2 + "_seq')";
            }

            public String getIdentityColumnString(int var1) {
                return var1 == -5 ? "bigserial not null" : "serial not null";
            }

            public boolean hasDataTypeInIdentityColumn() {
                return false;
            }
        }