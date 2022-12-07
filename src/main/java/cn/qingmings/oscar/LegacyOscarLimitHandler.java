package cn.qingmings.oscar;

import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.pagination.AbstractLimitHandler;
import org.hibernate.query.spi.Limit;

import java.util.regex.Matcher;

public class LegacyOscarLimitHandler extends AbstractLimitHandler {
    private final DatabaseVersion version;

    public LegacyOscarLimitHandler(DatabaseVersion version) {
        this.version = version;
    }


    @Override
    public String processSql(String sql, Limit limit) {
        final boolean hasOffset = hasFirstRow( limit );
        sql = sql.trim();

        String forUpdateClause = null;
        Matcher forUpdateMatcher = getForUpdatePattern().matcher( sql );
        if ( forUpdateMatcher.find() ) {
            int forUpdateIndex = forUpdateMatcher.start();
            // save 'for update ...' and then remove it
            forUpdateClause = sql.substring( forUpdateIndex );
            sql = sql.substring( 0, forUpdateIndex );
        }

        final StringBuilder pagingSelect = new StringBuilder( sql.length() + 100 );
        if ( hasOffset ) {
            pagingSelect.append( "select * from (select row_.*,rownum rownum_ from (" ).append( sql );
            if ( version.isBefore( 9 ) ) {
                pagingSelect.append( ") row_) where rownum_<=? and rownum_>?" );
            }
            else {
                pagingSelect.append( ") row_ where rownum<=?) where rownum_>?" );
            }
        }
        else {
            pagingSelect.append( "select * from (" ).append( sql ).append( ") where rownum<=?" );
        }

        if ( forUpdateClause != null ) {
            pagingSelect.append( forUpdateClause );
        }

        return pagingSelect.toString();
    }

    @Override
    public boolean bindLimitParametersInReverseOrder() {
        return true;
    }

    @Override
    public boolean supportsLimit() {
        return true;
    }

    @Override
    public boolean useMaxForLimit() {
        return true;
    }
}
