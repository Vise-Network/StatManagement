/*─────────────────────────────────────────────────────────────────────────────
 *  org.albi.core.data.StatManagement   (fixed Redis handling)
 *───────────────────────────────────────────────────────────────────────────*/

package org.albi.core.data;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bukkit.Bukkit;
import org.bukkit.plugin.Plugin;
import redis.clients.jedis.*;
import redis.clients.jedis.params.ZAddParams;

import java.lang.reflect.Type;
import java.sql.*;
import java.sql.Connection;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public final class StatManagement {

    /*─────────────────────────────────────────────────────────────────────────*/
    /*  PUBLIC FACADE (unchanged)                                             */
    /*─────────────────────────────────────────────────────────────────────────*/
    public static void init(Plugin plugin, Config cfg) {
        INTERNAL.store = new StatStore(plugin, cfg);
        INTERNAL.plugin = plugin;
    }
    public static CompletableFuture<PlayerStat> getAsync(UUID id, String name) {
        return CompletableFuture.supplyAsync(() ->
                INTERNAL.cache.computeIfAbsent(id, u -> INTERNAL.store.load(u, name))
        );
    }
    public static void saveAndRemove(UUID id) {
        PlayerStat ps = INTERNAL.cache.remove(id);
        if (ps != null)
            Bukkit.getScheduler().runTaskAsynchronously(
                    INTERNAL.plugin, () -> INTERNAL.store.save(ps));

        MainPlayer mp = INTERNAL.mainCache.remove(id);                     // NEW
        if (mp != null)
            Bukkit.getScheduler().runTaskAsynchronously(
                    INTERNAL.plugin, () -> INTERNAL.store.saveMain(mp));       // NEW
    }

    public static void flushAllAsync() {
        Bukkit.getScheduler().runTaskAsynchronously(INTERNAL.plugin, () -> {
            INTERNAL.cache.values().forEach(INTERNAL.store::save);
            INTERNAL.mainCache.values().forEach(INTERNAL.store::saveMain);  // NEW
        });
    }

    public static void resetWinStreak(UUID id, String game) {
        getAsync(id, "<unknown>").thenAccept(a -> a.resetWinStreak(game));
    }
    public static List<LBRow> leaderboard(String game, Field f, int n) {
        return INTERNAL.store.top(game, f.redis, n)
                .stream().map(e -> new LBRow(e.getKey(), e.getValue())).toList();
    }

    public static HikariDataSource getSql() {
        return INTERNAL.store.getSql();
    }

    public static RedisWrapper getRedis() {
        return INTERNAL.store.getRedis();
    }

    /*────────────────── MAIN‑PROFILE FACADE ──────────────────*/
    public static MainPlayer getMain(UUID id) {
        return INTERNAL.mainCache.computeIfAbsent(id, INTERNAL.store::loadMain);
    }

    public static CompletableFuture<MainPlayer> getMainAsync(org.bukkit.entity.Player p) {
        return CompletableFuture.supplyAsync(() ->
                INTERNAL.mainCache.computeIfAbsent(p.getUniqueId(), INTERNAL.store::loadMain)
        );
    }

    public static MainPlayer getMain(org.bukkit.entity.Player p) {
        return getMain(p.getUniqueId());
    }


    /*────────────────── internal glue ──────────────────*/
    private static final class INTERNAL {
        private static StatStore store;
        private static Plugin    plugin;

        private static final Map<UUID, PlayerStat> cache     = new ConcurrentHashMap<>();
        private static final Map<UUID, MainPlayer> mainCache = new ConcurrentHashMap<>();   // NEW
    }


    /*────────────────── simple records/enums ───────────*/
    public record Config(
            String mysqlJdbc, String mysqlUser, String mysqlPass,
            String redisHost, int redisPort, String redisPass){}

    public enum Field {
        KILLS("kills"), DEATHS("deaths"), WINS("wins"), TIME("time");
        public final String redis; Field(String r){this.redis=r;}
    }
    public record LBRow(UUID uuid,long score){}

    /*────────────────── stat beans (unchanged) ─────────*/
    public static final class Stat {
        private static final Gson G = new Gson();

        /* ── numeric fields ── */
        private int kills, deaths, wins, winStreak, roundsPlayed, timePlayed;

        /* ── increment helpers ── */
        public void addKills(int n)       { kills        += n; }
        public void addDeaths(int n)      { deaths       += n; }
        public void addWin() {            /* one win = +1 wins, +1 winStreak */
            wins++; winStreak++;
        }
        public void resetWinStreak()      { winStreak     = 0; }
        public void addRound()            { roundsPlayed += 1; }
        public void addTime(int seconds)  { timePlayed   += seconds; }

        /* ── getters (use in leaderboards or API) ── */
        public int kills()        { return kills; }
        public int deaths()       { return deaths; }
        public int wins()         { return wins; }
        public int winStreak()    { return winStreak; }
        public int roundsPlayed() { return roundsPlayed; }
        public int timePlayed()   { return timePlayed; }
    }

    public static final class PlayerStat {
        private final UUID uuid; private final Map<String,Stat> map=new HashMap<>();
        private final Set<String> dirtyGames=new HashSet<>(); private boolean dirty;
        public PlayerStat(UUID id){uuid=id;}
        public Stat game(String g){dirty=true;return map.computeIfAbsent(g,k->new Stat());}
        public void addKill(String g) { game(g).addKills(1);  dirtyGames.add(g); }
        public void addDeath(String g){ game(g).addDeaths(1); dirtyGames.add(g); }

        /* new: addWin = +wins and +winstreak */
        public void addWin(String g)  {
            Stat s = game(g);
            s.addWin();
            dirtyGames.add(g);
        }

        public void addRound(String g){ game(g).addRound();   dirtyGames.add(g); }
        public void addTime(String g,int s){ game(g).addTime(s); dirtyGames.add(g); }

        public void resetWinStreak(String g){
            game(g).resetWinStreak();
            dirtyGames.add(g);
        }
        public Map<String,Stat> all(){return map;} public UUID uuid(){return uuid;}
        public boolean isDirty(){return dirty;}  public void clearDirty(){dirty=false;dirtyGames.clear();}
        public Set<String> dirty(){return dirtyGames;}
    }

    /*════════════════════════════════════════════════════
     *   StatStore  (NOW WITH ROBUST REDIS WRAPPER)
     *══════════════════════════════════════════════════*/
    private static final class StatStore {

        /* ---- static constants ---- */
        private static final long TTL = Duration.ofHours(72).toSeconds();
        private static final Gson G   = new Gson();
        private static final Type MAP = new TypeToken<Map<String,Stat>>(){}.getType();

        /* ---- SQL ---- */
        private final HikariDataSource sql;

        /* ---- Redis wrapper ---- */
        private final RedisWrapper redis;

        /* ---- ctor ---- */
        StatStore(Plugin plugin, Config c) {
            this.redis = new RedisWrapper(plugin, c.redisHost(), c.redisPort(), c.redisPass());

            HikariConfig hc = new HikariConfig();
            hc.setJdbcUrl(c.mysqlJdbc());
            hc.setUsername(c.mysqlUser());
            hc.setPassword(c.mysqlPass());
            hc.setMaximumPoolSize(20);
            hc.setPoolName(plugin.getConfig().getString("region").toUpperCase()
                    + "-" + plugin.getConfig().getString("id_number"));
            sql = new HikariDataSource(hc);

            plugin.getLogger().info("[Stats] MySQL pool ready");

            setupDatabase(plugin);
        }

        private HikariDataSource getSql() {
            return sql;
        }

        private RedisWrapper getRedis() {
            return redis;
        }

        /*────────────────── MAIN‐PROFILE LOAD ──────────────────*/
        MainPlayer loadMain(UUID id) {
            String key = "main:" + id;

            /* 1️⃣  Redis cache */
            String json = redis.withJedis(j -> j.get(key));
            if (json != null) {
                redis.withJedis(j -> { j.expire(key, (int) TTL); return null; });
                return MainPlayer.fromJson(id, json);
            }

            /* 2️⃣  SQL fallback */
            try (Connection c = sql.getConnection();
                 PreparedStatement ps = c.prepareStatement(
                         "SELECT main FROM player_stats WHERE uuid=?")) {
                ps.setString(1, id.toString());
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    json = rs.getString(1);
                    final String cacheIt = json;
                    redis.withJedis(j -> { j.setex(key, (int) TTL, cacheIt); return null; });
                    return MainPlayer.fromJson(id, json);
                }
            } catch (SQLException ignored) { }
            return new MainPlayer(id);      // brand‑new profile
        }

        /*────────────────── MAIN‐PROFILE SAVE ──────────────────*/
        void saveMain(MainPlayer mp) {
            if (!mp.isDirty()) return;

            String key  = "main:" + mp.uuid();
            String json = mp.toJson();

            /* 1️⃣  Redis */
            redis.withJedis(j -> { j.setex(key, (int) TTL, json); return null; });

            /* 2️⃣  SQL (upsert) */
            try (Connection c = sql.getConnection();
                 PreparedStatement ps = c.prepareStatement(
                         """
                         INSERT INTO player_stats(uuid,stats,main)
                         VALUES(?, '{}', ?)                       -- '{}' placeholder for stats if new row
                         ON DUPLICATE KEY UPDATE main=VALUES(main)
                         """)) {
                ps.setString(1, mp.uuid().toString());
                ps.setString(2, json);
                ps.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            mp.clearDirty();
        }


        /* ensures table exists on first start */
        private void setupDatabase(Plugin plugin) {

            if(plugin.getConfig().getString("testing_mode", "no").equals("yes")) {
                String ddl = "DROP TABLE player_stats;";
                try (Connection c = sql.getConnection();
                     Statement  st = c.createStatement()) {
                    st.executeUpdate(ddl);
                    plugin.getLogger().info("[Stats TESTING] Successfully dropped SQL Table.");
                } catch (SQLException e) {
                    plugin.getLogger().severe("[Stats TESTING] Could not drop player_stats table: " + e.getMessage());
                }
            }

            String ddl = """
        CREATE TABLE IF NOT EXISTS player_stats (
          uuid  CHAR(36) PRIMARY KEY,
          stats JSON      NOT NULL,
          main JSON       NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """;
            try (Connection c = sql.getConnection();
                 Statement  st = c.createStatement()) {
                st.executeUpdate(ddl);
                plugin.getLogger().info("[Stats] Successfully initialized SQL Table.");
            } catch (SQLException e) {
                plugin.getLogger().severe("[Stats] Could not create player_stats table: " + e.getMessage());
            }
        }



        /* ───── load ───── */
        PlayerStat load(UUID id,String name){
            String key="stats:"+id;
            String js = redis.withJedis(j -> j.get(key));
            if(js!=null){
                redis.withJedis(j -> { j.expire(key,(int)TTL); return null;});
                return inflate(id,js);
            }
            /* cache miss -> SQL */
            try(Connection c=sql.getConnection(); PreparedStatement ps=c.prepareStatement(
                    "SELECT stats FROM player_stats WHERE uuid=?")){
                ps.setString(1,id.toString()); ResultSet rs=ps.executeQuery();
                if(rs.next()){ js=rs.getString(1);
                    String finalJs = js;
                    redis.withJedis(j -> { j.setex(key,(int)TTL, finalJs); return null;});
                    return inflate(id,js);
                }
            }catch(SQLException ignored){}
            return new PlayerStat(id);
        }

        /* ───── save ───── */
        void save(PlayerStat p){
            if(!p.isDirty()) return;
            String key="stats:"+p.uuid();
            String js=G.toJson(p.all());

            redis.withJedis(j -> {
                j.setex(key,(int)TTL,js);
                for(String g:p.dirty()){
                    Stat s=p.all().get(g);
                    zset(j, g, "kills",  s.kills(),       p.uuid());
                    zset(j, g, "wins",   s.wins(),        p.uuid());
                    zset(j, g, "streak", s.winStreak(),   p.uuid());
                    zset(j, g, "rounds", s.roundsPlayed(),p.uuid());

                }
                return null;
            });

            try(Connection c=sql.getConnection();PreparedStatement ps=c.prepareStatement(
                    "INSERT INTO player_stats(uuid,stats,main) VALUES(?,?, '{}') ON DUPLICATE KEY UPDATE stats=?")){
                ps.setString(1,p.uuid().toString()); ps.setString(2,js); ps.setString(3,js); ps.executeUpdate();
            }catch(SQLException e){e.printStackTrace();}
            p.clearDirty();
        }

        /* ───── leaderboard ───── */
        List<Map.Entry<UUID,Long>> top(String g,String f,int n){
            return redis.withJedis(j -> {
                List<Map.Entry<UUID,Long>> list=new ArrayList<>();
                j.zrevrangeWithScores(lb(g,f),0,n-1)
                        .forEach(t->list.add(Map.entry(UUID.fromString(t.getElement()),(long)t.getScore())));
                return list;
            });
        }

        /* ---- helpers ---- */
        private static PlayerStat inflate(UUID id,String js){
            PlayerStat p=new PlayerStat(id);
            p.all().putAll(G.fromJson(js,MAP));
            p.clearDirty();
            return p;
        }
        private static void zset(Jedis j,String g,String f,long score,UUID uuid){
            String z=lb(g,f);
            long updated = j.zadd(z, score, uuid.toString(), ZAddParams.zAddParams().xx().ch());
            if (updated == 0) {
                j.zadd(z, score, uuid.toString());   // first‑time insert
            }

            j.zremrangeByRank(z,100,-1);
        }
        private static String lb(String g,String f){return "lb:"+g.replace(' ','_')+":"+f;}
    }

    /*──────────────────────────────────────── RedisWrapper ────────────────*/
    public static final class RedisWrapper {
        private final Plugin plugin;
        private final String host; private final int port; private final String password;
        private JedisPool pool; private final JedisPoolConfig cfg=new JedisPoolConfig();

        RedisWrapper(Plugin plugin,String host,int port,String pwd){
            this.plugin=plugin; this.host=host; this.port=port; this.password=pwd;
            cfg.setMaxTotal(8); cfg.setMinIdle(1);
            cfg.setTestOnBorrow(true); cfg.setTestOnReturn(true);
            init();
        }
        /* open pool */
        private void init(){
            close();
            try{
                pool=new JedisPool(cfg,host,port,2000,password, true);
                try(Jedis j=pool.getResource()){ j.ping(); }
                plugin.getLogger().info("[Stats] Redis pool ready");
            }catch(Exception e){
                plugin.getLogger().severe("[Stats] Redis connect fail: "+e.getMessage());
                Bukkit.getPluginManager().disablePlugin(plugin);
            }
        }
        /* ensure pool healthy */
        private void ensure(){
            try(Jedis j=pool.getResource()){
                if(!"PONG".equalsIgnoreCase(j.ping())) throw new IllegalStateException("Bad ping");
            }catch(Exception ex){
                plugin.getLogger().warning("[Stats] Redis unhealthy, rebuilding…");
                init();
            }
        }
        /* public helpers */
        <R> R withJedis(Function<Jedis,R> fn){
            ensure();
            try(Jedis j=pool.getResource()){ return fn.apply(j); }
        }
        public void close(){ if(pool!=null && !pool.isClosed()) pool.close(); }
    }
}

// comments generated by chatgpt!
