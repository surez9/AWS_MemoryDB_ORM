using Amazon;
using Amazon.MemoryDB;
using Amazon.MemoryDB.Model;
using ESDMemoryORMTest.MemDB;
using ESDMemoryTest.MemDB;
using Redis.OM;
using StackExchange.Redis;
using static System.Net.Mime.MediaTypeNames;

namespace ESDMemoryTest;

public class Startup
{
    public Startup(IConfiguration configuration)
    {
        Configuration = configuration;
    }
    public AmazonMemoryDBClient DBClient { get; set; }

    public IConfiguration Configuration { get; }

    // This method gets called by the runtime. Use this method to add services to the container
    public void ConfigureServices(IServiceCollection services)
    {
        services.AddControllers();
        services.AddSwaggerGen();

        // Add MemoryDB connection
        services.AddSingleton<IConnectionMultiplexer>(sp =>
        {

            var primaryEndpoint = "clustercfg.orm-test.adb7tr.memorydb.us-east-1.amazonaws.com";
            var port = 6379;

           var config = new ConfigurationOptions
            {
                EndPoints = {
                        {  primaryEndpoint,port }
                    },
                Ssl = true,
                AbortOnConnectFail = false,
                ConnectTimeout = 5000
            };
            return ConnectionMultiplexer.Connect(config);
        });
 
        // Add Redis OM provider
        services.AddSingleton<RedisConnectionProvider>(sp =>
        {
            var muxer = sp.GetRequiredService<IConnectionMultiplexer>();
            return new RedisConnectionProvider(muxer);
        });
        services.AddHostedService<RedisOMIndexInitializer>();

        services.AddScoped<DBService>();
    }

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline
    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
        if (env.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }
        //Swagger
        app.UseSwagger();
        app.UseSwaggerUI(options =>
        {
            options.SwaggerEndpoint("/swagger/v1/swagger.json", "v1");
            options.RoutePrefix = string.Empty;
        });

        app.UseHttpsRedirection();

        app.UseRouting();

        app.UseAuthorization();

        app.UseEndpoints(endpoints =>
        {
            endpoints.MapControllers();
            endpoints.MapGet("/", async context =>
            {
                await context.Response.WriteAsync("Welcome to running ASP.NET Core on AWS Lambda");
            });
        });
    }
}