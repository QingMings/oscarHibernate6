package cn.qingmings.oscar;

import org.hibernate.boot.model.TypeContributions;
import org.hibernate.cfg.Environment;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.function.CommonFunctionFactory;
import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitLimitHandler;
import org.hibernate.dialect.sequence.SequenceSupport;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.query.sqm.produce.function.FunctionArgumentTypeResolver;
import org.hibernate.query.sqm.produce.function.StandardFunctionArgumentTypeResolvers;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tool.schema.extract.internal.SequenceInformationExtractorLegacyImpl;
import org.hibernate.tool.schema.extract.spi.SequenceInformationExtractor;
import org.hibernate.type.BasicTypeReference;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.spi.TypeConfiguration;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OscarDialect extends Dialect {


    public OscarDialect(DatabaseVersion version) {
        super(version);
    }

    public OscarDialect(DialectResolutionInfo info) {
        super(info);
    }

    public OscarDialect() {
        this(DatabaseVersion.make(7, 1));
    }

    @Override
    public void initializeFunctionRegistry(QueryEngine queryEngine) {
        super.initializeFunctionRegistry(queryEngine);
        final TypeConfiguration typeConfiguration = queryEngine.getTypeConfiguration();

        CommonFunctionFactory functionFactory = new CommonFunctionFactory(queryEngine);
        functionFactory.cot();
        functionFactory.log();
        functionFactory.cbrt();
        functionFactory.radians();
        functionFactory.degrees();
        functionFactory.stddev();
        functionFactory.variance();
        functionFactory.rand();
        functionFactory.trunc();
        functionFactory.ceiling_ceil();
        functionFactory.char_chr();
        functionFactory.char_chr();
        functionFactory.substr();
        functionFactory.substring_substr();
        functionFactory.leftRight_substr();
        functionFactory.initcap();

        registerFunction(queryEngine, typeConfiguration, "to_ascii",
                StandardBasicTypes.STRING, 1,
                StandardFunctionArgumentTypeResolvers.IMPLIED_RESULT_TYPE);
        registerFunction(queryEngine, typeConfiguration, "quote_ident",
                StandardBasicTypes.STRING, 1,
                StandardFunctionArgumentTypeResolvers.IMPLIED_RESULT_TYPE);
        registerFunction(queryEngine, typeConfiguration, "quote_literal",
                StandardBasicTypes.STRING, 1,
                StandardFunctionArgumentTypeResolvers.IMPLIED_RESULT_TYPE);
        functionFactory.ascii();
        registerFunction(queryEngine, typeConfiguration, "length",
                StandardBasicTypes.LONG, 1,
                StandardFunctionArgumentTypeResolvers.IMPLIED_RESULT_TYPE);
        registerFunction(queryEngine, typeConfiguration, "char_length",
                StandardBasicTypes.LONG, 1,
                StandardFunctionArgumentTypeResolvers.IMPLIED_RESULT_TYPE);
        registerFunction(queryEngine, typeConfiguration, "bit_length",
                StandardBasicTypes.LONG, 1,
                StandardFunctionArgumentTypeResolvers.IMPLIED_RESULT_TYPE);
        registerFunction(queryEngine, typeConfiguration, "octet_length",
                StandardBasicTypes.LONG, 1,
                StandardFunctionArgumentTypeResolvers.IMPLIED_RESULT_TYPE);
        functionFactory.nowCurdateCurtime();
        functionFactory.localtimeLocaltimestamp();
        registerNoArgFunction(queryEngine, typeConfiguration, "timeofday",
                StandardBasicTypes.STRING);
        registerFunction(queryEngine, typeConfiguration, "age",
                StandardBasicTypes.INTEGER, 1,
                StandardFunctionArgumentTypeResolvers.IMPLIED_RESULT_TYPE);
        registerNoArgFunction(queryEngine, typeConfiguration, "current_user",
                StandardBasicTypes.STRING);
        registerNoArgFunction(queryEngine, typeConfiguration, "session_user",
                StandardBasicTypes.STRING);
        registerNoArgFunction(queryEngine, typeConfiguration, "user",
                StandardBasicTypes.STRING);
        registerNoArgFunction(queryEngine, typeConfiguration, "current_database",
                StandardBasicTypes.STRING);
        registerNoArgFunction(queryEngine, typeConfiguration, "current_schema",
                StandardBasicTypes.STRING);
        functionFactory.toCharNumberDateTimestamp();
        functionFactory.concat();
        functionFactory.locate();
        registerFunction(queryEngine, typeConfiguration, "age",
                StandardBasicTypes.INTEGER, 1,
                StandardFunctionArgumentTypeResolvers.IMPLIED_RESULT_TYPE);
        queryEngine.getSqmFunctionRegistry().registerAlternateKey("str", "to_char");
    }

    @Override
    public String getAddColumnString() {
        return "add column";
    }

    @Override
    public SequenceInformationExtractor getSequenceInformationExtractor() {
        return SequenceInformationExtractorOscarDatabaseImpl.instance;
    }

//    Pool


    @Override
    protected void initDefaultProperties() {
        super.initDefaultProperties();
        getDefaultProperties().setProperty(Environment.STATEMENT_BATCH_SIZE, "15");
        getDefaultProperties().setProperty(Environment.NON_CONTEXTUAL_LOB_CREATION, "true");
    }

    @Override
    public int getDefaultStatementBatchSize() {
        return 15;
    }

    @Override
    protected String columnType(int sqlTypeCode) {
        switch (sqlTypeCode) {
            case SqlTypes.INTEGER:
                return "int";
            case SqlTypes.CHAR:
                return "char(1)";
            case SqlTypes.FLOAT:
                return "float4";
            case SqlTypes.DOUBLE:
                return "float8";
            case SqlTypes.TIMESTAMP:
                return "timestamp";
            case SqlTypes.LONGVARBINARY:
                return "blob";
            case SqlTypes.LONGVARCHAR:
                return "text";
        }
        return super.columnType(sqlTypeCode);
    }

    @Override
    protected void registerColumnTypes(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
        super.registerColumnTypes(typeContributions, serviceRegistry);
    }

    @Override
    public String getCascadeConstraintsString() {
        return " cascade";
    }

    @Override
    public boolean dropConstraints() {
        return true;
    }

    @Override
    public SequenceSupport getSequenceSupport() {
        return OscarSequenceSupport.INSTANCE;
    }

    @Override
    public String getQuerySequencesString() {
        return "select * from all_sequences";
    }




    @Override
    public LimitHandler getLimitHandler() {
        return LimitLimitHandler.INSTANCE;
    }

    @Override
    public boolean doesReadCommittedCauseWritersToBlockReaders() {
        return true;
    }

    @Override
    public boolean doesRepeatableReadCauseReadersToBlockWriters() {
        return true;
    }

    @Override
    public String getForUpdateString(String aliases) {
        return this.getForUpdateString() + " of " + aliases;
    }

    @Override
    public String getNativeIdentifierGeneratorStrategy() {
        return super.getNativeIdentifierGeneratorStrategy();
    }

    @Override
    public String getCaseInsensitiveLike() {
        return "ilike";
    }

    @Override
    public boolean supportsCaseInsensitiveLike() {
        return true;
    }

    @Override
    public String getNoColumnsInsertString() {
        return "default values";
    }

    @Override
    public boolean supportsOuterJoinForUpdate() {
        return false;
    }

    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    @Override
    public boolean useInputStreamToInsertBlob() {
        return false;
    }

    @Override
    public boolean supportsCommentOn() {
        return true;
    }

    @Override
    public boolean supportsCurrentTimestampSelection() {
        return true;
    }

    @Override
    public boolean isCurrentTimestampSelectStringCallable() {
        return false;
    }

    @Override
    public String getCurrentTimestampSelectString() {
        return "select now()";
    }

    @Override
    public String toBooleanValueString(boolean bool) {
        return bool ? "1" : "0";
    }

    @Override
    public boolean supportsSubselectAsInPredicateLHS() {

        return false;
    }

    @Override
    public boolean supportsExpectedLobUsagePattern() {
        return true;
    }

    @Override
    public boolean supportsLobValueChangePropagation() {
        return false;
    }

    @Override
    public ResultSet getResultSet(CallableStatement statement, int position) throws SQLException {
        if (position != 1) {
            throw new UnsupportedOperationException("Oscar only supports REF_CURSOR parameters as the first parameter");
        } else {
            return (ResultSet) statement.getObject(1);
        }
    }

    @Override
    public ResultSet getResultSet(CallableStatement statement, String name) throws SQLException {
        throw new UnsupportedOperationException("Oscar only supports accessing REF_CURSOR parameters by name");
    }

    @Override
    public boolean qualifyIndexName() {
        return false;
    }

    @Override
    public boolean supportsTupleCounts() {
        return false;
    }

    @Override
    public IdentityColumnSupport getIdentityColumnSupport() {
       return new OscarIdentityColumnSupport();
    }

    static class SequenceInformationExtractorOscarDatabaseImpl extends SequenceInformationExtractorLegacyImpl {
        private static final SequenceInformationExtractorOscarDatabaseImpl instance = new SequenceInformationExtractorOscarDatabaseImpl();

        SequenceInformationExtractorOscarDatabaseImpl() {
        }

        protected String sequenceCatalogColumn() {
            return null;
        }

        protected String sequenceSchemaColumn() {
            return null;
        }

        protected String sequenceStartValueColumn() {
            return null;
        }

        protected String sequenceMinValueColumn() {
            return "min_value";
        }

        protected Long resultSetMaxValue(ResultSet var1) throws SQLException {
            return var1.getBigDecimal("max_value").longValue();
        }

        protected String sequenceIncrementColumn() {
            return "increment_by";
        }
    }

    public <J> void registerFunction(
            QueryEngine queryEngine,
            TypeConfiguration typeConfiguration,
            String funcName,
            BasicTypeReference<J> basicTypeReference,
            int exactArgumentCount,
            FunctionArgumentTypeResolver argumentTypeResolver) {
        queryEngine.getSqmFunctionRegistry().namedDescriptorBuilder(funcName)
                .setInvariantType(typeConfiguration.getBasicTypeRegistry().resolve(basicTypeReference))
                .setExactArgumentCount(exactArgumentCount)
                .setArgumentTypeResolver(argumentTypeResolver)
                .register();
    }

    public <J> void registerNoArgFunction(
            QueryEngine queryEngine,
            TypeConfiguration typeConfiguration,
            String funcName,
            BasicTypeReference<J> basicTypeReference
    ) {
        queryEngine.getSqmFunctionRegistry().noArgsBuilder(funcName)
                .setInvariantType(typeConfiguration.getBasicTypeRegistry().resolve(basicTypeReference))
                .setUseParenthesesWhenNoArgs(true)
                .register();
    }
}
