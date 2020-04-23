import Vue from 'vue'
import VueRouter from 'vue-router'
import Layout from "../views/Layout";

Vue.use(VueRouter)

const routes = [
    {
        path: "/",
        name: 'Home',
        component: Layout,
    }
]

const router = new VueRouter({
    routes
})

export default router
