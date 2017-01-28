using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Azure.Search;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Expressions;
using Microsoft.EntityFrameworkCore.Query.ExpressionTranslators;
using Microsoft.EntityFrameworkCore.Query.ExpressionVisitors;
using Microsoft.EntityFrameworkCore.Query.ExpressionVisitors.Internal;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Remotion.Linq;
using Remotion.Linq.Clauses;

namespace EFAzure
{
    internal class Program
    {
        private static void Main()
        {
            #region Setup

            var serviceCollection
                = new ServiceCollection()
                    .AddEntityFrameworkSqlServer()
                    .AddScoped<IEntityQueryModelVisitorFactory, AzureSearchSqlServerQueryModelVisitorFactory>()
                    .AddScoped<IExpressionFragmentTranslator, AzureSearchRelationalCompositeExpressionFragmentTranslator>()
                    .AddScoped<ISqlTranslatingExpressionVisitorFactory, AzureSearchSqlTranslatingExpressionVisitorFactory>();

            var options
                = new DbContextOptionsBuilder()
                    .UseSqlServer("Data Source=(localdb)\\MSSQLLocalDB;Database=Northwind;Integrated Security=True")
                    .UseInternalServiceProvider(
                        serviceCollection
                            .BuildServiceProvider()).Options;

            #endregion

            using (var context = new NorthwindContext(options))
            {
                var results
                    = context.Customers
                        .Where(c => Azure.Search(c, "Lon*"));

                foreach (var customer in results)
                {
                    Console.WriteLine($"{customer.CustomerId} - {customer.CompanyName} - {customer.City}");
                }
            }

            Console.ReadKey();
        }
    }

    public class NorthwindContext : DbContext
    {
        public NorthwindContext(DbContextOptions options)
            : base(options)
        {
        }

        public DbSet<Customer> Customers { get; set; }

        public class Customer
        {
            public string CustomerId { get; set; }
            public string CompanyName { get; set; }
            public string ContactName { get; set; }
            public string ContactTitle { get; set; }
            public string Address { get; set; }
            public string City { get; set; }
            public string Region { get; set; }
            public string PostalCode { get; set; }
            public string Country { get; set; }
            public string Phone { get; set; }
            public string Fax { get; set; }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Customer>().ToTable("Customers");
    }

    public sealed class AzureSearchRelationalCompositeExpressionFragmentTranslator
        : RelationalCompositeExpressionFragmentTranslator
    {
        public AzureSearchRelationalCompositeExpressionFragmentTranslator(IModel model)
        {
            AddTranslators(new[] { new AzureSearchExpressionFragmentTranslator(model) });
        }
    }

    public class AzureSearchExpressionFragmentTranslator : IExpressionFragmentTranslator
    {
        private readonly IModel _model;

        public AzureSearchExpressionFragmentTranslator(IModel model)
        {
            _model = model;
        }

        public Expression Translate(Expression expression)
        {
            var methodCallExpression = expression as MethodCallExpression;

            if (methodCallExpression?.Method.Name == nameof(Azure.Search))
            {
                var keyProperty
                    = _model.FindEntityType(methodCallExpression.Arguments[0].Type)
                        .FindPrimaryKey().Properties.Single();

                var inExpression
                    = new InExpression(
                        Expression.Property(methodCallExpression.Arguments[0], keyProperty.PropertyInfo),
                        new[] { Expression.Parameter(keyProperty.ClrType, "azure_ids") });

                return inExpression;
            }

            return null;
        }
    }

    public class AzureSearchSqlServerQueryModelVisitor : SqlServerQueryModelVisitor
    {
        public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
        {
            base.VisitWhereClause(whereClause, queryModel, index);

            var predicateMethodCallExpression = whereClause.Predicate as MethodCallExpression;

            if (predicateMethodCallExpression?.Method.Name == nameof(Azure.Search))
            {
                var itemType = Expression.Type.GetGenericArguments()[0];
                var shapedQueryMethodCallExpression = (MethodCallExpression)Expression;
                var queryContextParameter = Expression.Parameter(shapedQueryMethodCallExpression.Arguments[0].Type);
                var shaperCommandContextParameter =
                    Expression.Parameter(shapedQueryMethodCallExpression.Arguments[1].Type);
                var shaperParameter = Expression.Parameter(typeof(IShaper<>).MakeGenericType(itemType));

                Expression
                    = Expression.Call(
                        _shapedQueryMethodInfo.MakeGenericMethod(
                            itemType),
                        shapedQueryMethodCallExpression.Arguments[0],
                        shapedQueryMethodCallExpression.Arguments[1],
                        shapedQueryMethodCallExpression.Arguments[2],
                        Expression.Lambda(
                            shapedQueryMethodCallExpression.Update(
                                shapedQueryMethodCallExpression.Object,
                                new[]
                                {
                                    queryContextParameter,
                                    shaperCommandContextParameter,
                                    shaperParameter
                                }),
                            queryContextParameter,
                            shaperCommandContextParameter,
                            shaperParameter),
                        predicateMethodCallExpression.Arguments[1]);
            }
        }

        private static readonly MethodInfo _shapedQueryMethodInfo
            = typeof(AzureSearchSqlServerQueryModelVisitor).GetTypeInfo()
                .GetDeclaredMethod(nameof(_ShapedQuery));

        // ReSharper disable once InconsistentNaming
        private static IEnumerable<T> _ShapedQuery<T>(
            QueryContext queryContext,
            ShaperCommandContext shaperCommandContext,
            IShaper<T> shaper,
            Func<QueryContext, ShaperCommandContext, IShaper<T>, IEnumerable<T>> inner,
            string query)
        {
            queryContext.AddParameter("azure_ids", GetIds(query));

            return inner(queryContext, shaperCommandContext, shaper);
        }

        private static string[] GetIds(string query)
        {
            using (var searchIndexClient
                = new SearchIndexClient(
                    "anpete-ef",
                    "customers-index",
                    new SearchCredentials("F16FAAFD3DD5F4F20E317F67DBE94406")))
            {
                var documentSearchResult = searchIndexClient.Documents.Search(query);

                return documentSearchResult.Results.Select(r => (string)r.Document["CustomerID"]).ToArray();
            }
        }

        #region Ctor

        public AzureSearchSqlServerQueryModelVisitor(
            IQueryOptimizer queryOptimizer,
            INavigationRewritingExpressionVisitorFactory navigationRewritingExpressionVisitorFactory,
            ISubQueryMemberPushDownExpressionVisitor subQueryMemberPushDownExpressionVisitor,
            IQuerySourceTracingExpressionVisitorFactory querySourceTracingExpressionVisitorFactory,
            IEntityResultFindingExpressionVisitorFactory entityResultFindingExpressionVisitorFactory,
            ITaskBlockingExpressionVisitor taskBlockingExpressionVisitor,
            IMemberAccessBindingExpressionVisitorFactory memberAccessBindingExpressionVisitorFactory,
            IOrderingExpressionVisitorFactory orderingExpressionVisitorFactory,
            IProjectionExpressionVisitorFactory projectionExpressionVisitorFactory,
            IEntityQueryableExpressionVisitorFactory entityQueryableExpressionVisitorFactory,
            IQueryAnnotationExtractor queryAnnotationExtractor,
            IRelationalResultOperatorHandler resultOperatorHandler,
            IEntityMaterializerSource entityMaterializerSource,
            IExpressionPrinter expressionPrinter,
            IRelationalAnnotationProvider relationalAnnotationProvider,
            IIncludeExpressionVisitorFactory includeExpressionVisitorFactory,
            ISqlTranslatingExpressionVisitorFactory sqlTranslatingExpressionVisitorFactory,
            ICompositePredicateExpressionVisitorFactory compositePredicateExpressionVisitorFactory,
            IConditionalRemovingExpressionVisitorFactory conditionalRemovingExpressionVisitorFactory,
            IQueryFlattenerFactory queryFlattenerFactory,
            IDbContextOptions contextOptions,
            RelationalQueryCompilationContext queryCompilationContext,
            AzureSearchSqlServerQueryModelVisitor parentQueryModelVisitor)
            : base(
                queryOptimizer,
                navigationRewritingExpressionVisitorFactory,
                subQueryMemberPushDownExpressionVisitor,
                querySourceTracingExpressionVisitorFactory,
                entityResultFindingExpressionVisitorFactory,
                taskBlockingExpressionVisitor,
                memberAccessBindingExpressionVisitorFactory,
                orderingExpressionVisitorFactory,
                projectionExpressionVisitorFactory,
                entityQueryableExpressionVisitorFactory,
                queryAnnotationExtractor,
                resultOperatorHandler,
                entityMaterializerSource,
                expressionPrinter,
                relationalAnnotationProvider,
                includeExpressionVisitorFactory,
                sqlTranslatingExpressionVisitorFactory,
                compositePredicateExpressionVisitorFactory,
                conditionalRemovingExpressionVisitorFactory,
                queryFlattenerFactory,
                contextOptions,
                queryCompilationContext,
                parentQueryModelVisitor)
        {
        }

        #endregion
    }

    public class AzureSearchSqlServerQueryModelVisitorFactory : SqlServerQueryModelVisitorFactory
    {
        public AzureSearchSqlServerQueryModelVisitorFactory(
            IQueryOptimizer queryOptimizer,
            INavigationRewritingExpressionVisitorFactory navigationRewritingExpressionVisitorFactory,
            ISubQueryMemberPushDownExpressionVisitor subQueryMemberPushDownExpressionVisitor,
            IQuerySourceTracingExpressionVisitorFactory querySourceTracingExpressionVisitorFactory,
            IEntityResultFindingExpressionVisitorFactory entityResultFindingExpressionVisitorFactory,
            ITaskBlockingExpressionVisitor taskBlockingExpressionVisitor,
            IMemberAccessBindingExpressionVisitorFactory memberAccessBindingExpressionVisitorFactory,
            IOrderingExpressionVisitorFactory orderingExpressionVisitorFactory,
            IProjectionExpressionVisitorFactory projectionExpressionVisitorFactory,
            IEntityQueryableExpressionVisitorFactory entityQueryableExpressionVisitorFactory,
            IQueryAnnotationExtractor queryAnnotationExtractor,
            IRelationalResultOperatorHandler resultOperatorHandler,
            IEntityMaterializerSource entityMaterializerSource,
            IExpressionPrinter expressionPrinter,
            IRelationalAnnotationProvider relationalAnnotationProvider,
            IIncludeExpressionVisitorFactory includeExpressionVisitorFactory,
            ISqlTranslatingExpressionVisitorFactory sqlTranslatingExpressionVisitorFactory,
            ICompositePredicateExpressionVisitorFactory compositePredicateExpressionVisitorFactory,
            IConditionalRemovingExpressionVisitorFactory conditionalRemovingExpressionVisitorFactory,
            IQueryFlattenerFactory queryFlattenerFactory,
            IDbContextOptions contextOptions)
            : base(queryOptimizer, navigationRewritingExpressionVisitorFactory,
                subQueryMemberPushDownExpressionVisitor,
                querySourceTracingExpressionVisitorFactory,
                entityResultFindingExpressionVisitorFactory,
                taskBlockingExpressionVisitor,
                memberAccessBindingExpressionVisitorFactory,
                orderingExpressionVisitorFactory,
                projectionExpressionVisitorFactory,
                entityQueryableExpressionVisitorFactory,
                queryAnnotationExtractor,
                resultOperatorHandler,
                entityMaterializerSource,
                expressionPrinter,
                relationalAnnotationProvider,
                includeExpressionVisitorFactory,
                sqlTranslatingExpressionVisitorFactory,
                compositePredicateExpressionVisitorFactory,
                conditionalRemovingExpressionVisitorFactory,
                queryFlattenerFactory,
                contextOptions)
        {
        }

        public override EntityQueryModelVisitor Create(
            QueryCompilationContext queryCompilationContext,
            EntityQueryModelVisitor parentEntityQueryModelVisitor)
            => new AzureSearchSqlServerQueryModelVisitor(
                QueryOptimizer,
                NavigationRewritingExpressionVisitorFactory,
                SubQueryMemberPushDownExpressionVisitor,
                QuerySourceTracingExpressionVisitorFactory,
                EntityResultFindingExpressionVisitorFactory,
                TaskBlockingExpressionVisitor,
                MemberAccessBindingExpressionVisitorFactory,
                OrderingExpressionVisitorFactory,
                ProjectionExpressionVisitorFactory,
                EntityQueryableExpressionVisitorFactory,
                QueryAnnotationExtractor,
                ResultOperatorHandler,
                EntityMaterializerSource,
                ExpressionPrinter,
                RelationalAnnotationProvider,
                IncludeExpressionVisitorFactory,
                SqlTranslatingExpressionVisitorFactory,
                CompositePredicateExpressionVisitorFactory,
                ConditionalRemovingExpressionVisitorFactory,
                QueryFlattenerFactory,
                ContextOptions,
                (RelationalQueryCompilationContext)queryCompilationContext,
                (AzureSearchSqlServerQueryModelVisitor)parentEntityQueryModelVisitor);
    }

    public static class Azure
    {
        public static bool Search<TItem>(TItem item, string query)
        {
            throw new InvalidOperationException();
        }
    }

    public class AzureSearchSqlTranslatingExpressionVisitorFactory : ISqlTranslatingExpressionVisitorFactory
    {
        private readonly IRelationalAnnotationProvider _relationalAnnotationProvider;
        private readonly IExpressionFragmentTranslator _compositeExpressionFragmentTranslator;
        private readonly IMethodCallTranslator _methodCallTranslator;
        private readonly IMemberTranslator _memberTranslator;
        private readonly IRelationalTypeMapper _relationalTypeMapper;

        public AzureSearchSqlTranslatingExpressionVisitorFactory(
            IRelationalAnnotationProvider relationalAnnotationProvider,
            IExpressionFragmentTranslator compositeExpressionFragmentTranslator,
            IMethodCallTranslator methodCallTranslator,
            IMemberTranslator memberTranslator,
            IRelationalTypeMapper relationalTypeMapper)
        {
            _relationalAnnotationProvider = relationalAnnotationProvider;
            _compositeExpressionFragmentTranslator = compositeExpressionFragmentTranslator;
            _methodCallTranslator = methodCallTranslator;
            _memberTranslator = memberTranslator;
            _relationalTypeMapper = relationalTypeMapper;
        }

        public virtual SqlTranslatingExpressionVisitor Create(
            RelationalQueryModelVisitor queryModelVisitor,
            SelectExpression targetSelectExpression = null,
            Expression topLevelPredicate = null,
            bool bindParentQueries = false,
            bool inProjection = false)
            => new AzureSearchSqlTranslatingExpressionVisitor(
                _relationalAnnotationProvider,
                _compositeExpressionFragmentTranslator,
                _methodCallTranslator,
                _memberTranslator,
                _relationalTypeMapper,
                queryModelVisitor,
                targetSelectExpression,
                topLevelPredicate,
                bindParentQueries,
                inProjection);
    }

    public class AzureSearchSqlTranslatingExpressionVisitor : SqlTranslatingExpressionVisitor
    {
        public AzureSearchSqlTranslatingExpressionVisitor(
            IRelationalAnnotationProvider relationalAnnotationProvider,
            IExpressionFragmentTranslator compositeExpressionFragmentTranslator,
            IMethodCallTranslator methodCallTranslator,
            IMemberTranslator memberTranslator,
            IRelationalTypeMapper relationalTypeMapper,
            RelationalQueryModelVisitor queryModelVisitor,
            SelectExpression targetSelectExpression = null,
            Expression topLevelPredicate = null,
            bool bindParentQueries = false,
            bool inProjection = false)
            : base(
                relationalAnnotationProvider, compositeExpressionFragmentTranslator, methodCallTranslator, memberTranslator,
                relationalTypeMapper, queryModelVisitor, targetSelectExpression, topLevelPredicate, bindParentQueries,
                inProjection)
        {
        }

        protected override Expression VisitExtension(Expression expression)
        {
            var newExpression = base.VisitExtension(expression);

            if (newExpression == null)
            {
                var inExpression = expression as InExpression;

                if (inExpression != null)
                {
                    return new InExpression(
                        Visit(inExpression.Operand),
                        inExpression.Values);
                }
            }

            return newExpression;
        }
    }
}