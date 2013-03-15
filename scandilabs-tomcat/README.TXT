To use a Tomcat "valve" like RewriteUrlValve, you need to add target/catamaran-tomc-x.x.jar to tomcat/lib.

Configure valves by adding this to the <Engine> section of tomcat/conf/server.xml:
<Valve className="org.catamarancode.tomcat.valve.rewriteurl.RewriteUrlValve" />

UPDATE: Use this configuration instead:
<Valve className="org.catamarancode.tomcat.valve.rewriteurl.StaticHostRewriteUrlValve" staticHostName="mydomain.com" />